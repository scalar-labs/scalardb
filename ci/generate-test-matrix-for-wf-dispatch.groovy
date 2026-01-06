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
// If a test has a "versions" array, expand it into multiple test variants
def testsToRun = []
testsByCategory.each { category, tests ->
    tests.each { test ->
        test["test_category"] = category
        if (test.versions) {
            // Expand test into multiple variants, one per version
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

// Output the tests to run as JSON for the matrix strategy
def jsonBuilder = new JsonBuilder([include: testsToRun])
println(jsonBuilder.toString())
