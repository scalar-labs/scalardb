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

// test_sources paths are relative to the repository root
def repositoryRoot = new File(scriptBasePath.resolve(".."))
testsByCategory.each { category, tests ->
    tests.findAll { it.test_sources }.each { test ->
        def testClasses = findTestClasses(test, repositoryRoot)
        resolveRemainingTests(test, testClasses)
        validateTestGroupsCoverage(test, testClasses)
    }
}

// Expand tests by versions, and then by test groups or group commit settings
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

// Expand tests by versions, and then by test groups or group commit settings
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

            // Expand each variant by test group, or by group commit setting
            testVariants.each { variant ->
                if (variant.test_groups) {
                    expandedTests.addAll(expandTestGroups(variant))
                    return
                }

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

// Expand a test with test groups into one test per group, each running only the group's test classes
def static expandTestGroups(test) {
    if (test.containsKey("disable_group_commit")) {
        exitWithError("Test '${test.label}': test_groups cannot be combined with disable_group_commit")
    }

    return test.test_groups.collect { group ->
        if (group.remaining_tests && !test.test_sources) {
            exitWithError("Test '${test.label}', group '${group.label}': remaining_tests requires test_sources")
        }
        def missingFields = ["label", "name", "group_commit_enabled", "tests"].findAll {
            group[it] == null || (it == "tests" && group.tests.isEmpty())
        }
        if (!missingFields.isEmpty()) {
            exitWithError("Test '${test.label}', group '${group.label}': missing ${missingFields.join(', ')}")
        }

        def groupTest = new LinkedHashMap(test)
        groupTest.remove("test_groups")
        groupTest.remove("test_sources")
        groupTest["label"] = "${test.label}_${group.label}".toString()
        groupTest["display_name"] = "${test.display_name} (${group.name})".toString()
        groupTest["run"] = test.run.trim() + group.tests.collect { " --tests ${it}" }.join()
        groupTest["group_commit_enabled"] = group.group_commit_enabled.toString()
        groupTest["report_group"] = test.label
        return groupTest
    }
}

// List the test classes in test_sources
def static findTestClasses(test, File repositoryRoot) {
    return test.test_sources.collectMany { source ->
        def dir = new File(repositoryRoot, source)
        if (!dir.isDirectory()) {
            exitWithError("Test '${test.label}': test source '${source}' is not a directory")
        }
        dir.listFiles().findAll { it.name.endsWith(".java") && isTestClass(it) }
                .collect { it.name - ".java" }
    } as Set
}

// Give a group with remaining_tests the test classes that the other groups with the same group
// commit setting don't list. A group commit group gets only the ConsensusCommit test classes, which
// are the ones the group commit jobs of tests without test groups run
def static resolveRemainingTests(test, Set testClasses) {
    test.test_groups.groupBy { it.group_commit_enabled.toString() == 'true' }.each { groupCommitEnabled, groups ->
        def remainingGroups = groups.findAll { it.remaining_tests }
        if (remainingGroups.isEmpty()) {
            return
        }
        def flavor = groupCommitEnabled ? "with group commit" : "without group commit"
        if (remainingGroups.size() > 1) {
            exitWithError("Test '${test.label}': more than one test group ${flavor} has remaining_tests: ${remainingGroups*.label.join(', ')}")
        }
        def remainingGroup = remainingGroups[0]
        if (remainingGroup.tests != null) {
            exitWithError("Test '${test.label}', group '${remainingGroup.label}' ${flavor}: remaining_tests cannot be combined with tests")
        }
        def candidates = groupCommitEnabled ? testClasses.findAll { it.startsWith("ConsensusCommit") } : testClasses
        def listed = groups.collectMany { it.tests ?: [] }
        def remaining = (candidates - listed).sort()
        // A group without tests would run the whole test task
        if (remaining.isEmpty()) {
            exitWithError("Test '${test.label}', group '${remainingGroup.label}' ${flavor}: no test class remains for remaining_tests")
        }
        remainingGroup.tests = remaining
    }
}

// Check that the test groups run every test class in test_sources, and that
// the group commit groups run every ConsensusCommit test class, which the group commit jobs of
// tests without test groups select with the **.ConsensusCommit** filter. Also check that no test
// class is listed more than once for the same group commit setting
def static validateTestGroupsCoverage(test, Set testClasses) {
    def (groupCommitGroups, defaultGroups) = test.test_groups.split { it.group_commit_enabled.toString() == 'true' }
    def defaultClasses = defaultGroups.collectMany { it.tests ?: [] } as Set
    def groupCommitClasses = groupCommitGroups.collectMany { it.tests ?: [] } as Set
    def errors = []
    def unlisted = testClasses - defaultClasses
    if (unlisted) {
        errors.add("test classes missing from the test groups without group commit: ${unlisted.sort().join(', ')}")
    }
    def unknown = (defaultClasses + groupCommitClasses) - testClasses
    if (unknown) {
        errors.add("test_groups list classes that are not in test_sources: ${unknown.sort().join(', ')}")
    }
    def groupCommitUnlisted = testClasses.findAll { it.startsWith("ConsensusCommit") } - groupCommitClasses
    if (groupCommitUnlisted) {
        errors.add("ConsensusCommit test classes missing from the group commit test groups: ${groupCommitUnlisted.sort().join(', ')}")
    }
    [["without group commit", defaultGroups], ["with group commit", groupCommitGroups]].each { flavor, groups ->
        def duplicated = groups.collectMany { it.tests ?: [] }.countBy { it }.findAll { it.value > 1 }.keySet()
        if (duplicated) {
            errors.add("test classes listed more than once in the test groups ${flavor}: ${duplicated.sort().join(', ')}")
        }
    }
    if (errors) {
        exitWithError("Test '${test.label}': ${errors.join('. ')}")
    }
}

// Test class names contain "Test", while helper class names end with "Utils", such as JdbcTestUtils,
// or have no "Test", such as JdbcEnv. Matching names rather than test annotations catches test
// classes that only inherit their test methods.
def static isTestClass(File javaFile) {
    def className = javaFile.name - ".java"
    if (!className.contains("Test") || className.endsWith("Utils")) {
        return false
    }
    return !(javaFile.text =~ /\babstract\s+(?:\w+\s+)*class\s+${className}\b/)
}

static def exitWithError(String message) {
    System.err.println("Error: ${message}")
    System.exit(1)
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
        // At minimum, only the integration tests for PostgreSQL 17, Dynamo, and Multi-Storage are
        // run when the CI is triggered on a PR. PostgresSQL 17, and Dynamo are the
        // fastest tests among RDBMS and NoSQL storages, respectively, and multi-storage provides
        // test provide test coverage for a feature that is not tested by the others.
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
