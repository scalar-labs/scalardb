#!/usr/bin/env groovy

// This script generates a test matrix for the integration tests based on the environment variables

import groovy.yaml.YamlSlurper
import groovy.json.JsonBuilder
import groovy.transform.SourceURI

// Check for required environment variables
def requiredEnvVars = [
        'ALL',
        'BLOB_STORAGE',
        'CASSANDRA',
        'COSMOS',
        'DYNAMO',
        'JDBC',
        'MULTI_STORAGE',
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

// Read input parameters from environment variables
def allTest = Boolean.parseBoolean(System.getenv('ALL'))
def blobStorageTest = Boolean.parseBoolean(System.getenv('BLOB_STORAGE'))
def cassandraTest = Boolean.parseBoolean(System.getenv('CASSANDRA'))
def cosmosTest = Boolean.parseBoolean(System.getenv('COSMOS'))
def dynamoTest = Boolean.parseBoolean(System.getenv('DYNAMO'))
def jdbcTest = Boolean.parseBoolean(System.getenv('JDBC'))
def multiStorageTest = Boolean.parseBoolean(System.getenv('MULTI_STORAGE'))

// Load the tests organized by category from YAML file
@SourceURI
URI scriptBasePath
def testsByCategory = new YamlSlurper().parse(new File(scriptBasePath.resolve("tests.yaml")))

// Filter the tests based on inputs - remove the disabled categories
if (!allTest) {
    if (!blobStorageTest) {
        testsByCategory.remove("blob-storage")
    }
    if (!cassandraTest) {
        testsByCategory.remove("cassandra")
    }
    if (!cosmosTest) {
        testsByCategory.remove("cosmos")
    }
    if (!dynamoTest) {
        testsByCategory.remove("dynamo")
    }
    if (!jdbcTest) {
        testsByCategory.remove("jdbc")
    }
    if (!multiStorageTest) {
        testsByCategory.remove("multi-storage")
    }
}

// Collect all remaining tests from enabled categories
// If a test has a "versions" array, create a test entry for each version
def testsToRun = []
testsByCategory.each { category, tests ->
    tests.each { test ->
        test["test_category"] = category
        if (test.versions) {
            // For each test variant that has several versions, create a separate entry for each version
            test.versions.each { version ->
                def testVariant = new LinkedHashMap(test)
                testVariant.remove("versions")
                testVariant["version"] = version
                testsToRun.add(testVariant)
            }
        } else {
            testsToRun.add(test)
        }
    }
}

// For each test "without disable_group_commit:true", create two entries, one where "group_commit_enabled: true", one where "group_commit_enabled: false"
def expandedTests = []
testsToRun.each { test ->
    if (test.disable_group_commit) {
        // Keep as is, remove the disable_group_commit attribute
        def testVariant = new LinkedHashMap(test)
        testVariant.remove("disable_group_commit")
        testVariant["group_commit_enabled"] = false
        expandedTests.add(testVariant)
    } else {
        // Create two variants: one with group_commit_enabled true, one with false
        def testWithGroupCommit = new LinkedHashMap(test)
        testWithGroupCommit["group_commit_enabled"] = true
        expandedTests.add(testWithGroupCommit)

        def testWithoutGroupCommit = new LinkedHashMap(test)
        testWithoutGroupCommit["group_commit_enabled"] = false
        expandedTests.add(testWithoutGroupCommit)
    }
}

// Output the tests to run as JSON for the matrix strategy
def jsonBuilder = new JsonBuilder([include: expandedTests])
println(jsonBuilder.toString())
