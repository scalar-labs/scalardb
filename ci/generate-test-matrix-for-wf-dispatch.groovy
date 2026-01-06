#!/usr/bin/env groovy

// This script generates a test matrix for the node integration tests based on the environment variables

@Grab('org.json:json:20250517')

import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.transform.SourceURI

// Check for required environment variables
def requiredEnvVars = [
        'ALL',
        'ABAC',
        'AUTH',
        'BASIC',
        'EMBEDDING',
        'ENCRYPTION',
        'REPLICATION'
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
def abacTest = Boolean.parseBoolean(System.getenv('ABAC'))
def authTest = Boolean.parseBoolean(System.getenv('AUTH'))
def basicTest = Boolean.parseBoolean(System.getenv('BASIC'))
def embeddingTest = Boolean.parseBoolean(System.getenv('EMBEDDING'))
def encryptionTest = Boolean.parseBoolean(System.getenv('ENCRYPTION'))
def replicationTest = Boolean.parseBoolean(System.getenv('REPLICATION'))

// Load the tests organized by category from JSON file
@SourceURI
URI scriptBasePath
def testsByCategory = new JsonSlurper().parse(new File(scriptBasePath.resolve("node_tests_by_category.json")))

// Filter the tests based on inputs - remove the disabled categories
if (!allTest) {
    if (!abacTest) {
        testsByCategory.remove("abac")
    }
    if (!authTest) {
        testsByCategory.remove("auth")
    }
    if (!basicTest) {
        testsByCategory.remove("basic")
    }
    if (!embeddingTest) {
        testsByCategory.remove("embedding")
    }
    if (!encryptionTest) {
        testsByCategory.remove("encryption")
    }
    if (!replicationTest) {
        testsByCategory.remove("replication")
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
