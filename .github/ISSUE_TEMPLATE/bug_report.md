---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''
---

**Describe the bug**

A clear and concise description of what the bug is.

**To Reproduce**

Steps to reproduce the behavior:

1. Prepare the following config file:
```properties
scalar.db.storage=...
scalar.db.contact_points=...
scalar.db.username=...
scalar.db.password=...
```
2. Prepare the following schema file:
```json
{
  "sample_db.sample_table": {
    "partition-key": [
      "c1"
    ],
    "clustering-key": [
      "c2 ASC"
    ],
    "columns": {
      "c1": "INT",
      "c2": "TEXT",
      "c3": "INT",
      "c4": "INT"
    },
    "secondary-index": [
      "c4"
    ],
  }
}
```
3. Load the schema with Schema Loader
4. Execute the following code snippet:
```java
// the code snippet to reproduce the behavior
```
5. See error
```java
// the error
```

**Expected behavior**

A clear and concise description of what you expected to happen.

**Screenshots**

If applicable, add screenshots to help explain your problem.

**Environment (please complete the following information):**

 - OS: [e.g. masOS Monterey 12.5]
 - Java Version: [e.g. 1.8.0_202]
 - ScalarDB Version: [e.g. 3.7.0]

**Additional context**

Add any other context about the problem here.
