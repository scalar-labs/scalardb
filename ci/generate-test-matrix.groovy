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

// Filter tests based on environment variables
expandedTestsConfig = filterTestsByEnvVar(expandedTestsConfig)

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

// Validate that all required environment variables are set
static def validateEnvironmentVariables() {
    def requiredEnvVars = [
            'ALL',
            'BASIC',
            'BLOB_STORAGE',
            'CASSANDRA',
            'COSMOS',
            'DYNAMO',
            'JDBC',
            'MULTI_STORAGE',
            'EXPANDED_TESTS_CONFIG_FILE'
    ]

    def missingVars = []
    requiredEnvVars.each { varName ->
        if (System.getenv(varName) == null) {
            missingVars.add(varName)
        }
    }

    if (!missingVars.isEmpty()) {
        System.err.println("Error: Required environment variables not set: ${missingVars.join(', ')}")
        System.exit(1)
    }
}

// Expand tests by versions and group commit settings
def static expandTests(testsByCategory) {
    def expandedTests = []

    testsByCategory.each { category, tests ->
        tests.each { test ->
            test["test_category"] = category

            // Get list of test variants (expanded by version if applicable)
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
                    testVariants.add(testVariant)
                }
            } else {
                testVariants.add(test)
            }

            // Expand each variant by group commit setting
            testVariants.each { variant ->
                def baseVariant = new LinkedHashMap(variant)
                baseVariant.remove("disable_group_commit")

                def testWithoutGroupCommit = new LinkedHashMap(baseVariant)
                testWithoutGroupCommit["group_commit_enabled"] = 'false'
                expandedTests.add(testWithoutGroupCommit)

                if (!variant.disable_group_commit) {
                    def testWithGroupCommit = new LinkedHashMap(baseVariant)
                    testWithGroupCommit["group_commit_enabled"] = 'true'
                    expandedTests.add(testWithGroupCommit)
                }
            }
        }
    }

    return expandedTests
}

// Filter tests based on environment variables
def static filterTestsByEnvVar(expandedTests) {
    // If ALL is true, return all tests without filtering
    if (Boolean.parseBoolean(System.getenv('ALL'))) {
        return expandedTests
    }

    // Build lists of test categories and labels to keep based on env vars
    def testCategoriesToKeep = [] as Set
    def testLabelsToKeep = [] as Set

    if (Boolean.parseBoolean(System.getenv('BASIC'))) {
        testLabelsToKeep.add("postgresql_17")
        testCategoriesToKeep.add("dynamo")
        testCategoriesToKeep.add("multi-storage")
    }

    def envVarToCategory = [
            'BLOB_STORAGE' : 'blob-storage',
            'CASSANDRA'    : 'cassandra',
            'COSMOS'       : 'cosmos',
            'DYNAMO'       : 'dynamo',
            'JDBC'         : 'jdbc',
            'MULTI_STORAGE': 'multi-storage'
    ]

    envVarToCategory.each { envVar, category ->
        if (Boolean.parseBoolean(System.getenv(envVar))) {
            testCategoriesToKeep.add(category)
        }
    }

    // Filter tests to keep only those in the categories or labels to keep
    return expandedTests.findAll { test ->
        testCategoriesToKeep.contains(test.test_category) || testLabelsToKeep.contains(test.label)
    }
}
