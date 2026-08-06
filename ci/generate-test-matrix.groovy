#!/usr/bin/env groovy

// This script generates a test matrix for the integration tests based on the environment variables
// It outputs:
// 1. A JSON file with the full expanded test configuration (written to a file)
// 2. A minimal JSON matrix with just labels for GitHub Actions (printed to stdout)

import groovy.yaml.YamlSlurper
import groovy.json.JsonBuilder
import groovy.transform.SourceURI

validateEnvironmentVariables()

// Load the tests organized by category from YAML file
@SourceURI
URI scriptBasePath
def testsByCategory = new YamlSlurper().parse(new File(scriptBasePath.resolve("tests-config.yaml")))

// Expand tests by versions and group commit settings
def expandedTestsConfig = expandTests(testsByCategory)

// Select the tests to run based on the TESTS environment variable
expandedTestsConfig = selectTests(expandedTestsConfig)

// Output the full expanded test configuration as JSON to a file
// This file will be shared with integration test jobs as a build artifact. This is necessary because
// Github does not allow outputting passwords (contained in setup and run attribute) in the matrix
// directly because they are flagged as sensitive data
def expandedTestsConfigFilePath = System.getenv('EXPANDED_TESTS_CONFIG_FILE');
def fullConfigJson = new JsonBuilder(expandedTestsConfig)
new File(expandedTestsConfigFilePath).text = fullConfigJson.toPrettyString()

// Output a minimal JSON matrix with just labels, display_name and group_commit_enabled for GitHub Actions
def minimalMatrix = expandedTestsConfig.collect { test ->
    [
            label               : test.label,
            display_name        : test.display_name,
            group_commit_enabled: test.group_commit_enabled,
            runner              : test.runner ?: 'ubuntu-latest'
    ]
}
def jsonBuilder = new JsonBuilder([include: minimalMatrix])
println(jsonBuilder.toString())

// Utility methods declaration

// Validate that all required environment variables are set. Empty values are allowed: the
// pull_request and workflow_call paths render FLAVOR as an empty string, which means 'all'.
static def validateEnvironmentVariables() {
    def requiredEnvVars = ['TESTS', 'FLAVOR', 'EXPANDED_TESTS_CONFIG_FILE']

    def missingVars = requiredEnvVars.findAll { System.getenv(it) == null }

    if (!missingVars.isEmpty()) {
        System.err.println("Error: Required environment variables not set: ${missingVars.join(', ')}")
        System.exit(1)
    }
}

// Expand tests by versions and flavors
def static expandTests(testsByCategory) {
    def expandedTests = []

    testsByCategory.each { category, tests ->
        tests.each { test ->
            test["test_category"] = category

            def flavor = test.flavor ?: 'all'
            if (!(flavor in ['all', 'group-commit', 'normal'])) {
                System.err.println("Error: Unknown flavor '${flavor}' for storage '${test.storage}' in tests-config.yaml.")
                System.err.println("Valid flavors: all, group-commit, normal")
                System.exit(1)
            }

            // Get list of test variants (expanded by version if applicable). The label is derived
            // from the storage and the version rather than being declared in the config.
            def testVariants = []
            if (test.versions) {
                test.versions.each { version ->
                    def testVariant = new LinkedHashMap(test)
                    testVariant.remove("versions")
                    // Replace %VERSION% placeholder with the actual version in string properties
                    testVariant.each { key, value ->
                        if (value instanceof String) {
                            testVariant[key] = value.replace('%VERSION%', version.toString())
                        }
                    }
                    testVariant["label"] = "${test.storage}_${version}".toString()
                    testVariants.add(testVariant)
                }
            } else {
                def testVariant = new LinkedHashMap(test)
                testVariant["label"] = test.storage
                testVariants.add(testVariant)
            }

            // Expand each variant by flavor
            testVariants.each { variant ->
                def baseVariant = new LinkedHashMap(variant)
                baseVariant.remove("flavor")

                if (flavor != 'group-commit') {
                    def testWithoutGroupCommit = new LinkedHashMap(baseVariant)
                    testWithoutGroupCommit["group_commit_enabled"] = 'false'
                    expandedTests.add(testWithoutGroupCommit)
                }
                if (flavor != 'normal') {
                    def testWithGroupCommit = new LinkedHashMap(baseVariant)
                    testWithGroupCommit["group_commit_enabled"] = 'true'
                    expandedTests.add(testWithGroupCommit)
                }
            }
        }
    }

    return expandedTests
}

// The basic set is the cheapest meaningful coverage: PostgreSQL 17 and DynamoDB are the fastest
// RDBMS and NoSQL tests respectively, and multi-storage covers a feature neither exercises.
// This is a method rather than a field: a Groovy script cannot declare a static field at top
// level, and a static method cannot see an @Field instance field.
static List<String> basicLabels() { ['postgresql_17', 'dynamo', 'multi-storage'] }

// Print an error and abort. Every validation failure goes through here so the format is uniform.
static def failWith(String message) {
    System.err.println("Error: ${message}")
    System.exit(1)
}

static def failUnknownValue(String value, Set storages, Set categories) {
    def valid = (['all', 'basic'] + categories.toList() + storages.toList()).unique().sort()
    failWith("Unknown value '${value}' in the TESTS input.\n" +
            "Valid values: ${valid.join(', ')}\n" +
            "  (append ':<version>' to a storage to select one version)")
}

// Resolve the TESTS input into the list of tests to run.
static def selectTests(expandedTests) {
    def tokens = (System.getenv('TESTS') ?: '')
            .split(',')
            .collect { it.trim().toLowerCase() }
            .findAll { it }

    // An empty input means everything, and 'all' wins over any other token.
    if (tokens.isEmpty() || tokens.contains('all')) {
        return expandedTests
    }

    def storages = expandedTests.collect { it.storage }.toSet()
    def categories = expandedTests.collect { it.test_category }.toSet()
    def labelsByStorage = expandedTests.groupBy { it.storage }
            .collectEntries { storage, tests -> [(storage): tests.collect { it.label }.toSet()] }

    def missingBasic = basicLabels().findAll { label -> !expandedTests.any { it.label == label } }
    if (!missingBasic.isEmpty()) {
        failWith("The 'basic' set refers to labels that no longer exist in tests-config.yaml: " +
                "${missingBasic.join(', ')}.\n  Update basicLabels() in generate-test-matrix.groovy.")
    }

    def labelsToKeep = [] as Set

    tokens.each { token ->
        if (token == 'basic') {
            labelsToKeep.addAll(basicLabels())
            return
        }

        if (token.contains(':')) {
            def parts = token.split(':', 2)
            def storage = parts[0]
            def version = parts[1]

            if (!storages.contains(storage)) {
                if (categories.contains(storage)) {
                    failWith("'${storage}' is a category and has no versions, so '${token}' is invalid.\n" +
                            "  Use '${storage}', or a storage such as 'mysql:8.0'.")
                }
                failUnknownValue(storage, storages, categories)
            }

            def storageLabels = labelsByStorage[storage]
            if (storageLabels == ([storage] as Set)) {
                failWith("Storage '${storage}' has no versions, so '${token}' is invalid.\n" +
                        "  Use '${storage}'.")
            }

            def wantedLabel = "${storage}_${version}".toString()
            if (!storageLabels.contains(wantedLabel)) {
                def validVersions = storageLabels.collect { it.substring(storage.length() + 1) }.sort()
                failWith("Storage '${storage}' has no version '${version}' in the TESTS input.\n" +
                        "Valid versions for '${storage}': ${validVersions.join(', ')}")
            }
            labelsToKeep.add(wantedLabel)
            return
        }

        if (storages.contains(token)) {
            labelsToKeep.addAll(labelsByStorage[token])
            return
        }
        if (categories.contains(token)) {
            labelsToKeep.addAll(expandedTests.findAll { it.test_category == token }.collect { it.label })
            return
        }
        failUnknownValue(token, storages, categories)
    }

    return expandedTests.findAll { labelsToKeep.contains(it.label) }
}
