#!/usr/bin/env groovy

// This script generates a test matrix for the integration tests based on the environment variables

@Grab('org.json:json:20250517')

import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.transform.SourceURI

// Check for required environment variables
def requiredEnvVars = [
        'ALL',
        'BLOBSTORAGE',
        'CASSANDRA',
        'COSMOS',
        'DYNAMO',
        'JDBC',
        'MULTISTORAGE',
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
def blobstorageTest = Boolean.parseBoolean(System.getenv('BLOBSTORAGE'))
def cassandraTest = Boolean.parseBoolean(System.getenv('CASSANDRA'))
def cosmosTest = Boolean.parseBoolean(System.getenv('COSMOS'))
def dynamoTest = Boolean.parseBoolean(System.getenv('DYNAMO'))
def jdbcTest = Boolean.parseBoolean(System.getenv('JDBC'))
def multistorageTest = Boolean.parseBoolean(System.getenv('MULTISTORAGE'))

// Load the tests organized by category from JSON file
@SourceURI
URI scriptBasePath
def testsByCategory = new JsonSlurper().parse(new File(scriptBasePath.resolve("node_tests_by_category.json")))

// Filter the tests based on inputs - remove the disabled categories
if (!allTest) {
    if (!blobstorageTest) {
        testsByCategory.remove("blobstorage")
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
    if (!multistorageTest) {
        testsByCategory.remove("multistorage")
    }
}

// Collect all remaining tests from enabled categories
def testsToRun = []
testsByCategory.each { category, tests ->
    tests.each { test ->
        // Check for duplicates before adding
        if (!testsToRun.any { it.label == test.label }) {
            test["test_category"] = category
            testsToRun.add(test)
        }
    }
}

// Output the tests to run as JSON for the matrix strategy
def jsonBuilder = new JsonBuilder([include: testsToRun])
println(jsonBuilder.toString())
