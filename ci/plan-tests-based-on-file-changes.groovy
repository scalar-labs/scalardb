#!/usr/bin/env groovy

// This script sets the TESTS environment variable indicating which tests to run based on which
// storage adapter packages have changed files.

import groovy.transform.Field

// Check for required environment variable
if (System.getenv("PR_BASE_BRANCH") == null) {
    System.err.println("Error: Required environment variable not set: PR_BASE_BRANCH")
    System.exit(1)
}

@Field final String ADAPTER_PACKAGE_DIR = 'core/src/main/java/com/scalar/db/storage/'

def changedDirectories = getChangedAdapterPackageDirectories()
initializeEnvVar(changedDirectories)

// Utility methods declaration

// Get the folders containing file changes
def getChangedAdapterPackageDirectories() {
    def baseBranch = System.getenv("PR_BASE_BRANCH")
    executeCommand("git fetch origin ${baseBranch}")

    // Get the list of changed files against the PR base branch
    def proc = executeCommand("git diff --name-only origin/${baseBranch} HEAD")
    def changedDirectories = proc.text.readLines()
            .findAll { it.trim() }
            .findAll { it.startsWith(ADAPTER_PACKAGE_DIR) }  // Filter files under the storage adapter path
            .collect { filePath ->
                // Remove the ADAPTER_PACKAGE_DIR prefix and get the first directory segment
                def relativePath = filePath.substring(ADAPTER_PACKAGE_DIR.length())
                def parts = relativePath.split('/')
                return parts[0]
            }.unique()

    return changedDirectories
}

// Write the TESTS environment variable based on the changed adapter directories
def initializeEnvVar(changedDirectories) {
    def githubEnvFile = System.getenv("GITHUB_ENV")

    if (!githubEnvFile) {
        System.err.println("Error: GITHUB_ENV environment variable is not set")
        System.exit(1)
    }

    def directoryToCategory = [
            'objectstorage': 'blob-storage',
            'cassandra'    : 'cassandra',
            'cosmos'       : 'cosmos',
            'dynamo'       : 'dynamo',
            'jdbc'         : 'jdbc'
    ]

    // The basic tests set always runs. It already covers multi-storage, so that category is never
    // added explicitly.
    def tests = ['basic']
    directoryToCategory.each { directory, category ->
        if (changedDirectories.contains(directory)) {
            tests.add(category)
        }
    }

    new File(githubEnvFile).withWriterAppend { writer ->
        writer.writeLine("TESTS=${tests.join(',')}")
    }
}

// Execute a command and check for errors
def executeCommand(String command) {
    def proc = command.execute()
    proc.waitFor()

    if (proc.exitValue() != 0) {
        System.err.println("Error: Command failed: ${command}")
        System.err.println("Exit code: ${proc.exitValue()}")
        System.err.println("Error output: ${proc.err.text}")
        System.exit(1)
    }

    return proc
}
