# Technical Specification

# 0. Agent Action Plan

## 0.1 Intent Clarification

Based on the provided requirements, the Blitzy platform understands that the testing objective is to **implement a comprehensive, non-redundant test suite covering 15 critical user workflows** across HDFS, YARN, MapReduce, and Common components within the Apache Hadoop codebase. This is a **"Add new tests"** category request focused on creating integration and unit tests using MiniCluster infrastructure.

### 0.1.1 Core Testing Objective

The user's requirements translate to the following primary goals:

- **Coverage Target:** Achieve 80% line coverage on seven targeted packages across four Hadoop components
- **Workflow Completeness:** Exercise 100% of 15 enumerated critical workflows end-to-end with validation
- **Test Budget:** Deliver ≤250 total tests with zero redundancy (no duplicate input-operation-validation tuples)
- **Time Constraint:** Execute full suite within 45-minute CI budget with single test execution ≤60 seconds
- **Infrastructure Integration:** Leverage existing Hadoop MiniCluster infrastructure (MiniDFSCluster, MiniYARNCluster, MiniMRCluster, MiniKDC)

The target packages for coverage are:

| Component | Package Path | Focus Area |
|-----------|--------------|------------|
| HDFS | `org.apache.hadoop.hdfs` | NameNode, DataNode client operations |
| HDFS | `org.apache.hadoop.fs` | FileSystem API user interactions |
| YARN | `org.apache.hadoop.yarn.client` | Application submission, status |
| YARN | `org.apache.hadoop.yarn.api` | Resource management interfaces |
| MapReduce | `org.apache.hadoop.mapreduce` | Job lifecycle, execution |
| Common | `org.apache.hadoop.conf` | Configuration loading |
| Common | `org.apache.hadoop.security` | Authentication workflows |

### 0.1.2 Special Instructions and Constraints

The user has explicitly emphasized the following directives:

**Minimal Change Principle:**
- "ONLY modify test files and test-related configurations"
- "DO NOT modify source code unless absolutely necessary for testability"
- Add test files exclusively in designated `workflow/` directories
- Do not modify existing test classes or test utilities
- Do not refactor production code for testability

**Production Function Exclusivity (CRITICAL):**
- All tests MUST invoke production functions directly—zero tolerance for reimplemented logic
- Mock/stub ONLY external dependencies (APIs, databases, file systems)—never internal functions under test
- Test assertions validate return values and side effects of actual production code paths
- No inline recreation of algorithms, calculations, or transformations that exist in source

**Static Cluster Reuse:**
- All test classes MUST extend the appropriate `Abstract*WorkflowTest` base class
- MiniCluster initialization costs 15-30 seconds per instance—static reuse across test methods is required

**Async Patterns:**
- Use `GenericTestUtils.waitFor()` for all async state transitions—zero `Thread.sleep()` calls
- Use `@ParameterizedTest` for scenarios differing only by input values to avoid duplicate test methods

### 0.1.3 Technical Interpretation

These testing requirements translate to the following technical test implementation strategy:

- **To test HDFS file operations**, create `TestFileCreateWriteWorkflow`, `TestFileReadWorkflow`, `TestFileDeleteWorkflow`, `TestDirectoryOperationsWorkflow`, and `TestPermissionAclWorkflow` in `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/`
- **To test YARN application lifecycle**, create `TestApplicationSubmissionWorkflow`, `TestResourceAllocationWorkflow`, `TestApplicationCompletionWorkflow`, and `TestApplicationStatusWorkflow` in `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/`
- **To test MapReduce job execution**, create `TestJobSubmissionWorkflow`, `TestMapPhaseWorkflow`, `TestReducePhaseWorkflow`, and `TestJobFailureWorkflow` in `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/`
- **To test Common utilities**, create `TestConfigurationLoadingWorkflow` in `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/` and `TestKerberosAuthenticationWorkflow` in `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/`

### 0.1.4 Coverage Requirements Interpretation

**Explicit coverage targets mentioned by user:**
- 80% line coverage on targeted packages
- 100% workflow path completion for all 15 critical workflows

**Implicit coverage expectations based on analysis:**
- Industry standards for Java testing typically target 70-80% line coverage for critical paths
- Existing Hadoop test patterns focus on integration tests using MiniClusters for realistic validation
- Critical path analysis indicates emphasis on:
  - Happy path scenarios for all workflows
  - Edge cases (zero-byte files, block boundaries, resource limits)
  - Error handling (invalid inputs, timeout scenarios, failure recovery)

**To achieve comprehensive testing, coverage should include:**
- All public methods in targeted FileSystem, YarnClient, and Job APIs
- Block boundary conditions (exact block size, multi-block, sub-block)
- Async state transitions with proper wait conditions
- Permission and security validation paths
- Error and exception handling code paths

## 0.2 Test Discovery and Analysis

### 0.2.1 Existing Test Infrastructure Assessment

Repository analysis reveals a **JUnit 5 (Jupiter)** testing setup with extensive MiniCluster-based integration testing infrastructure. The Hadoop project uses Maven Surefire for test execution with static cluster lifecycle management patterns.

**Testing Framework Stack Discovered:**

| Component | Version | Location |
|-----------|---------|----------|
| JUnit Jupiter | 5.13.3 | `hadoop-project/pom.xml` |
| JUnit Platform | 1.13.3 | `hadoop-project/pom.xml` |
| Mockito Core | 4.11.0 | `hadoop-project/pom.xml` |
| AssertJ Core | 3.12.2 | `hadoop-project/pom.xml` |
| Hamcrest | 2.2 | `hadoop-project/pom.xml` |

**MiniCluster Infrastructure Identified:**

| Cluster Type | Location | Purpose |
|--------------|----------|---------|
| MiniDFSCluster | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSCluster.java` | In-process HDFS with NameNode and DataNodes |
| MiniYARNCluster | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/MiniYARNCluster.java` | In-process ResourceManager, NodeManagers, Timeline |
| MiniMRYarnCluster | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/v2/MiniMRYarnCluster.java` | MapReduce over YARN |
| MiniKDC | `hadoop-common-project/hadoop-minikdc/src/main/java/org/apache/hadoop/minikdc/MiniKdc.java` | Embedded Kerberos KDC |

**Critical Test Utilities Discovered:**

| Utility Class | Location | Key Methods |
|---------------|----------|-------------|
| GenericTestUtils | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/test/GenericTestUtils.java` | `waitFor()`, `assertExceptionContains()`, `LogCapturer` |
| LambdaTestUtils | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/test/LambdaTestUtils.java` | `await()`, `intercept()`, `eventually()` |
| DFSTestUtil | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java` | File creation, block operations, wait methods |
| MiniDFSClusterWithNodeGroup | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSClusterWithNodeGroup.java` | Topology-aware testing |

**Existing Test Patterns Relevant to Workflows:**

| Test Pattern | Example File | Pattern Description |
|--------------|--------------|---------------------|
| Static MiniDFSCluster | `TestDFSInputStream.java` | `@BeforeAll` cluster init, `@AfterAll` shutdown |
| Parameterized Tests | `TestConfiguration.java` | `@ParameterizedTest` with `@MethodSource` |
| HA Failover Tests | `TestYarnClientOnRMFailover.java` | RM failover simulation with MiniYARNCluster |
| Security Tests | `TestUserGroupInformation.java` | MiniKDC with UGI login patterns |

### 0.2.2 Web Search Research Conducted

**JUnit 5 and Mockito Compatibility:**
- JUnit Jupiter 5.13.x is fully compatible with Java 17 runtime
- Mockito 4.11.0 is compatible with Java 17; Mockito 5.x provides enhanced JDK 17+ support but is not required
- The `mockito-junit-jupiter` integration artifact enables `@ExtendWith(MockitoExtension.class)` patterns

**GenericTestUtils.waitFor() Best Practices:**
- Use lambda expressions for condition checks: `GenericTestUtils.waitFor(() -> condition, checkIntervalMs, timeoutMs)`
- Always specify meaningful timeout values (100ms check interval, 30000ms timeout typical)
- Avoid nested waits; prefer single waitFor with compound conditions
- Include diagnostic message parameter for debugging flaky tests

**MiniDFSCluster Best Practices:**
- Use `MiniDFSCluster.Builder(conf).numDataNodes(n).build()` pattern
- Call `cluster.waitActive()` after build to ensure DataNodes register
- Use try-with-resources or explicit `@AfterAll` shutdown
- Configure block size and replication in `HdfsConfiguration` before cluster init

**MiniYARNCluster Best Practices:**
- Wait for NodeManagers to register using `GenericTestUtils.waitFor()` after cluster start
- Use `YarnClient.createYarnClient()` for application submission tests
- Allow 30-60 second timeouts for application state transitions

### 0.2.3 Existing Test Coverage Gaps Identified

Based on repository analysis, the following workflow-specific test gaps exist:

| Workflow | Existing Coverage | Gap Analysis |
|----------|-------------------|--------------|
| HDFS File Create/Write | Partial (TestDFSOutputStream) | Missing parameterized block boundary tests |
| HDFS File Read | Partial (TestDFSInputStream) | Missing positional read and seek workflow tests |
| HDFS Directory Ops | Scattered across multiple tests | Missing unified workflow test |
| YARN App Submission | Partial (TestYarnClient) | Missing end-to-end workflow with state validation |
| MR Job Lifecycle | Partial (TestMRJobs) | Missing phased completion workflow tests |
| Configuration Loading | Extensive (TestConfiguration) | Variable substitution edge cases needed |
| Kerberos Auth | Partial (TestUserGroupInformation) | Missing end-to-end doAs workflow |

The new `workflow/` test directories will provide focused, non-redundant coverage for these specific user scenarios without duplicating existing test assertions.

## 0.3 Testing Scope Analysis

### 0.3.1 Test Target Identification

**Primary Code to Be Tested:**

| Module/Class | Path | Test Categories Needed |
|--------------|------|------------------------|
| FileSystem (HDFS) | `org.apache.hadoop.fs.FileSystem` | Unit + Integration |
| FSDataOutputStream | `org.apache.hadoop.fs.FSDataOutputStream` | Integration |
| FSDataInputStream | `org.apache.hadoop.fs.FSDataInputStream` | Integration |
| DistributedFileSystem | `org.apache.hadoop.hdfs.DistributedFileSystem` | Integration |
| YarnClient | `org.apache.hadoop.yarn.client.api.YarnClient` | Integration |
| YarnClientImpl | `org.apache.hadoop.yarn.client.api.impl.YarnClientImpl` | Integration |
| AMRMClient | `org.apache.hadoop.yarn.client.api.AMRMClient` | Integration |
| Job | `org.apache.hadoop.mapreduce.Job` | Integration |
| Configuration | `org.apache.hadoop.conf.Configuration` | Unit |
| UserGroupInformation | `org.apache.hadoop.security.UserGroupInformation` | Integration |

**Existing Test File Mapping:**

| Source File | Existing Test File | Test Categories Present |
|-------------|-------------------|-------------------------|
| `FileSystem.java` | `TestFileSystem.java`, `TestDFS.java` | Basic operations, no workflow focus |
| `DistributedFileSystem.java` | `TestDistributedFileSystem.java` | API coverage, scattered scenarios |
| `YarnClientImpl.java` | `TestYarnClient.java` | Basic client ops, no lifecycle workflow |
| `Job.java` | `TestMRJobs.java`, `TestLocalJobSubmission.java` | Job execution, limited phase tracking |
| `Configuration.java` | `TestConfiguration.java` | Comprehensive, missing edge workflows |
| `UserGroupInformation.java` | `TestUserGroupInformation.java` | Auth patterns, missing doAs workflow |

**Dependencies Requiring Mocking:**

| Dependency Type | Components to Mock/Stub | Approach |
|-----------------|------------------------|----------|
| External Services | HTTP endpoints, RPC calls | MiniCluster provides in-process |
| Database Interactions | N/A for core Hadoop | Not applicable |
| File System Operations | Virtualized via MiniDFSCluster | MiniDFSCluster instance |
| Network Calls | Inter-DataNode, NameNode RPC | MiniCluster handles in-process |
| Time-Based Operations | Heartbeats, lease expiry | Configuration-based acceleration |

### 0.3.2 Version Compatibility Research

Based on the repository's `hadoop-project/pom.xml` and web search verification, the recommended testing stack for Java 17 compatibility is:

**Testing Framework Versions (From Repository):**

| Component | Repository Version | Rationale |
|-----------|-------------------|-----------|
| JUnit Jupiter | 5.13.3 | Already configured in `hadoop-project/pom.xml`; fully Java 17 compatible |
| JUnit Platform | 1.13.3 | Matches Jupiter version; provides Surefire integration |
| Mockito Core | 4.11.0 | Already configured; Java 17 compatible without requiring mockito-inline |
| AssertJ | 3.12.2 | Already configured; fluent assertions support |

**MiniCluster Compatibility:**

| Cluster Type | Init Time (Observed) | Instances Planned | Total Overhead |
|--------------|---------------------|-------------------|----------------|
| MiniDFSCluster | ~10s | 5 test classes | ~50s |
| MiniYARNCluster | ~25s | 4 test classes | ~100s |
| MiniMRCluster | ~20s | 4 test classes | ~80s |
| MiniKDC | ~5s | 1 test class | ~5s |
| **Total Cluster Overhead** | | | **~4 minutes** |

**Remaining Test Execution Budget:** ~41 minutes for 250 tests = ~10 seconds average per test

**Version Conflicts to Resolve:** None identified. The existing dependency tree in `hadoop-project/pom.xml` manages transitive dependencies correctly.

### 0.3.3 Critical Workflow Analysis

**HDFS Workflows (5):**

| Workflow ID | Name | Production APIs Exercised | Edge Cases |
|-------------|------|--------------------------|------------|
| 1 | File Create/Write/Close | `FileSystem.create()`, `FSDataOutputStream.write()`, `close()` | Zero-byte, exact block, multi-block, overwrite |
| 2 | File Read (Full, Seek, Positional) | `FileSystem.open()`, `read()`, `seek()`, `readFully()` | Read beyond EOF, seek boundaries, concurrent reads |
| 3 | File Delete (Single, Recursive) | `FileSystem.delete()` | Delete non-existent, non-empty directory |
| 4 | Directory Operations | `mkdirs()`, `listStatus()`, `rename()` | mkdir existing, rename to existing, list empty |
| 5 | Permission/ACL Modification | `setPermission()`, `modifyAclEntries()`, `setAcl()` | Remove all ACLs, permission denied, sticky bit |

**YARN Workflows (4):**

| Workflow ID | Name | Production APIs Exercised | Edge Cases |
|-------------|------|--------------------------|------------|
| 6 | Application Submission | `YarnClient.createApplication()`, `submitApplication()` | Invalid resource request, duplicate submission |
| 7 | Resource Request/Allocation | `AMRMClient.addContainerRequest()`, `allocate()` | Impossible request, relaxed locality |
| 8 | Application Completion | `unregisterApplicationMaster()`, `killApplication()` | Timeout kill, AM crash |
| 9 | Application Status Query | `YarnClient.getApplicationReport()` | Non-existent app, post-RM restart |

**MapReduce Workflows (4):**

| Workflow ID | Name | Production APIs Exercised | Edge Cases |
|-------------|------|--------------------------|------------|
| 10 | Job Submission | `Job.submit()`, `waitForCompletion()` | Missing input, invalid config |
| 11 | Map Phase Completion | `Job.getCounters()`, `getTaskReports()` | Map failure with retry, speculative execution |
| 12 | Reduce Phase Completion | Monitor reduce progress, output validation | Zero reducers, combiner verification |
| 13 | Job Failure Handling | `waitForCompletion()` returns false | Partial map completion, AM failure |

**Common Workflows (2):**

| Workflow ID | Name | Production APIs Exercised | Edge Cases |
|-------------|------|--------------------------|------------|
| 14 | Configuration Loading | `addResource()`, `get()`, `getInt()`, `getBoolean()` | Missing property, circular substitution, final override |
| 15 | Kerberos Authentication | `loginUserFromKeytab()`, `doAs()` | Expired ticket, invalid keytab |

## 0.4 Test Implementation Design

### 0.4.1 Test Strategy Selection

**Test Types to Implement:**

| Test Type | Count Ceiling | Purpose | Distribution |
|-----------|---------------|---------|--------------|
| Integration (MiniCluster) | ≤150 | Workflow validation with realistic components | 60% |
| Unit | ≤100 | Edge cases, failure injection, isolated logic | 40% |

**Test Focus by Category:**

- **Unit Tests:** Focus on isolated component behavior, configuration parsing, edge case input validation
- **Integration Tests:** Cover end-to-end workflow paths using MiniCluster infrastructure
- **Edge Case Tests:** Address boundary conditions (block sizes, resource limits, empty inputs)
- **Error Handling Tests:** Verify failure scenarios, exception messages, recovery behavior

### 0.4.2 Test Case Blueprint

**HDFS Workflow Tests:**

```
Component: FileSystem (HDFS File Operations)
Test Class: TestFileCreateWriteWorkflow
Test Categories:
- Happy path: Create file, write bytes, close, verify metadata
- Edge cases: Zero-byte file, exactly one block, multi-block, overwrite
- Error cases: Write to non-existent parent, permission denied
Production APIs: FileSystem.create(), FSDataOutputStream.write(), getFileStatus()
```

```
Component: FileSystem (Read Operations)
Test Class: TestFileReadWorkflow
Test Categories:
- Happy path: Open file, read full content, verify checksum
- Edge cases: Seek to block boundary, positional read, read beyond EOF
- Error cases: Open non-existent file, read from closed stream
Production APIs: FileSystem.open(), FSDataInputStream.read(), seek(), readFully()
```

```
Component: FileSystem (Delete Operations)
Test Class: TestFileDeleteWorkflow
Test Categories:
- Happy path: Delete file, delete empty directory, recursive delete
- Edge cases: Delete non-existent returns false, non-recursive on non-empty
- Error cases: Delete permission denied, delete root
Production APIs: FileSystem.delete(), exists()
```

**YARN Workflow Tests:**

```
Component: YarnClient (Application Lifecycle)
Test Class: TestApplicationSubmissionWorkflow
Test Categories:
- Happy path: Create app, submit, wait for RUNNING state
- Edge cases: Invalid resource request rejection, duplicate submission
- Error cases: Submit to unavailable RM
Production APIs: createApplication(), submitApplication(), getApplicationReport()
```

```
Component: AMRMClient (Resource Allocation)
Test Class: TestResourceAllocationWorkflow
Test Categories:
- Happy path: Request containers, allocate, verify resources
- Edge cases: Impossible resource request, locality relaxation
- Error cases: Allocation timeout
Production APIs: addContainerRequest(), allocate()
```

**MapReduce Workflow Tests:**

```
Component: Job (Job Lifecycle)
Test Class: TestJobSubmissionWorkflow
Test Categories:
- Happy path: Configure job, submit, verify JobId
- Edge cases: Missing input path fast fail
- Error cases: Invalid mapper class
Production APIs: Job.submit(), getJobState(), getJobID()
```

```
Component: Job (Phase Completion)
Test Class: TestMapPhaseWorkflow
Test Categories:
- Happy path: Submit job, monitor map completion
- Edge cases: Map task retry on failure, speculative execution
- Error cases: All maps fail
Production APIs: getCounters(), getTaskReports(TaskType.MAP)
```

### 0.4.3 Existing Test Extension Strategy

This test suite creates **new** workflow-focused tests in dedicated directories. No existing tests are extended or modified per the minimal change principle.

**Tests to Create (not extend):**
- All 15 workflow test classes in new `workflow/` directories
- 5 abstract base classes for cluster lifecycle management

**Tests NOT to Modify:**
- Existing `Test*.java` files in standard test directories
- Existing test utilities in `org.apache.hadoop.test`

### 0.4.4 Test Data and Fixtures Design

**Required Test Data Structures:**

| Data Type | Purpose | Generation Strategy |
|-----------|---------|---------------------|
| Byte arrays | File content for write/read tests | Deterministic: `new Random(42).nextBytes()` |
| File paths | Unique test directories | `TestInfo.getDisplayName()` + timestamp |
| Configuration | Cluster configuration | `HdfsConfiguration`, `YarnConfiguration` |
| Keytab files | Kerberos authentication | MiniKDC `createPrincipal()` |

**Fixture Organization Strategy:**

- **Cluster Lifecycle:** Static `@BeforeAll`/`@AfterAll` in abstract base classes
- **Test Isolation:** Per-test directory creation in `@BeforeEach`, cleanup in `@AfterEach`
- **Test Data:** Embedded byte arrays for small content (≤1MB), generated via `RandomDatum` for large
- **Unique Namespaces:** `/workflow/[TestClassName]/[methodName]/` pattern

**Mock Object Specifications:**

| Mock Target | Mock Type | Usage |
|-------------|-----------|-------|
| External HTTP | Not needed | MiniCluster provides in-process RPC |
| DataNode failures | `DataNodeFaultInjector` | Built-in Hadoop fault injection |
| Time/Clock | Not needed | Configuration-based timeouts |

**Test Database/State Management Approach:**

- No external databases required; MiniDFSCluster provides HDFS state
- Cleanup strategy: Delete test directories in `@AfterEach`, not `@AfterClass` (ensures isolation)
- State validation: Query production APIs (getFileStatus, getApplicationReport) not internal state

### 0.4.5 Abstract Base Class Pattern

**Mandatory Pattern for MiniCluster Reuse:**

All test classes must extend the appropriate abstract base class to achieve the 45-minute execution budget:

| Base Class | MiniCluster Type | Init Time | Shared Resources |
|------------|------------------|-----------|------------------|
| `AbstractHdfsWorkflowTest` | MiniDFSCluster | ~10s | `cluster`, `fs`, `conf` |
| `AbstractYarnWorkflowTest` | MiniYARNCluster | ~25s | `yarnCluster`, `yarnClient`, `conf` |
| `AbstractMapReduceWorkflowTest` | MiniMRYarnCluster | ~20s | `dfsCluster`, `mrCluster`, `fs`, `conf` |
| `AbstractCommonWorkflowTest` | None | N/A | `conf` fixture |
| `AbstractSecurityWorkflowTest` | MiniKDC | ~5s | `kdc`, `keytabFile`, `principal` |

**Base Class Responsibilities:**

```
AbstractHdfsWorkflowTest:
- @BeforeAll: Initialize MiniDFSCluster with 3 DataNodes
- @AfterAll: Shutdown cluster and close FileSystem
- @BeforeEach: Create unique test directory in /workflow namespace
- @AfterEach: Delete test directory recursively
- Provide: cluster, conf, fs, testDir as protected fields
```

```
AbstractYarnWorkflowTest:
- @BeforeAll: Initialize MiniYARNCluster, wait for NodeManagers
- @AfterAll: Stop YarnClient and cluster
- Provide: yarnCluster, yarnClient, conf as protected fields
```

```
AbstractSecurityWorkflowTest:
- @BeforeAll: Initialize MiniKDC, create test principal and keytab
- @AfterAll: Stop KDC, delete work directory
- Provide: kdc, keytabFile, principal as protected fields
```

## 0.5 Test File Transformation Mapping

### 0.5.1 File-by-File Test Plan

**Test Transformation Modes:**
- **CREATE** - Create a new test file
- **UPDATE** - Update an existing test file
- **DELETE** - Remove an obsolete test file
- **REFERENCE** - Use as an example for test patterns and styles

**HDFS Workflow Test Files:**

| Target Test File | Transformation | Source/Reference | Purpose/Changes |
|-----------------|----------------|------------------|-----------------|
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/AbstractHdfsWorkflowTest.java` | CREATE | `TestDFSInputStream.java`, `TestMiniDFSCluster.java` | Base class with static MiniDFSCluster lifecycle, test namespace management |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestFileCreateWriteWorkflow.java` | CREATE | `org.apache.hadoop.hdfs.DistributedFileSystem` | Parameterized tests for Workflow 1: zero-byte, sub-block, exact-block, multi-block writes |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestFileReadWorkflow.java` | CREATE | `org.apache.hadoop.fs.FSDataInputStream` | Parameterized tests for Workflow 2: full read, seek, positional read, EOF handling |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestFileDeleteWorkflow.java` | CREATE | `org.apache.hadoop.fs.FileSystem` | Tests for Workflow 3: single file delete, recursive directory delete, edge cases |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestDirectoryOperationsWorkflow.java` | CREATE | `org.apache.hadoop.fs.FileSystem` | Tests for Workflow 4: mkdirs, listStatus, rename with edge cases |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestPermissionAclWorkflow.java` | CREATE | `org.apache.hadoop.hdfs.DistributedFileSystem` | Tests for Workflow 5: setPermission, modifyAclEntries, setAcl, access checks |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java` | REFERENCE | N/A | Reference for HDFS test utility patterns |
| `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestMiniDFSCluster.java` | REFERENCE | N/A | Reference for MiniDFSCluster initialization patterns |

**YARN Workflow Test Files:**

| Target Test File | Transformation | Source/Reference | Purpose/Changes |
|-----------------|----------------|------------------|-----------------|
| `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/AbstractYarnWorkflowTest.java` | CREATE | `TestYarnClient.java` | Base class with static MiniYARNCluster lifecycle, NodeManager wait logic |
| `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestApplicationSubmissionWorkflow.java` | CREATE | `org.apache.hadoop.yarn.client.api.YarnClient` | Tests for Workflow 6: createApplication, submitApplication, state validation |
| `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestResourceAllocationWorkflow.java` | CREATE | `org.apache.hadoop.yarn.client.api.AMRMClient` | Tests for Workflow 7: addContainerRequest, allocate, resource validation |
| `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestApplicationCompletionWorkflow.java` | CREATE | `org.apache.hadoop.yarn.client.api.YarnClient` | Tests for Workflow 8: unregisterApplicationMaster, killApplication, resource release |
| `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestApplicationStatusWorkflow.java` | CREATE | `org.apache.hadoop.yarn.client.api.YarnClient` | Tests for Workflow 9: getApplicationReport, state queries, diagnostics |
| `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestYarnClient.java` | REFERENCE | N/A | Reference for YarnClient test patterns |

**MapReduce Workflow Test Files:**

| Target Test File | Transformation | Source/Reference | Purpose/Changes |
|-----------------|----------------|------------------|-----------------|
| `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/AbstractMapReduceWorkflowTest.java` | CREATE | `MiniMRYarnCluster.java` | Base class with static MiniMRCluster + MiniDFSCluster lifecycle |
| `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestJobSubmissionWorkflow.java` | CREATE | `org.apache.hadoop.mapreduce.Job` | Tests for Workflow 10: job configuration, submit, JobId validation |
| `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestMapPhaseWorkflow.java` | CREATE | `org.apache.hadoop.mapreduce.Job` | Tests for Workflow 11: map task completion, counters, speculative execution |
| `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestReducePhaseWorkflow.java` | CREATE | `org.apache.hadoop.mapreduce.Job` | Tests for Workflow 12: reduce completion, output validation, combiner verification |
| `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestJobFailureWorkflow.java` | CREATE | `org.apache.hadoop.mapreduce.Job` | Tests for Workflow 13: job failure handling, diagnostics, cleanup |
| `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/v2/MiniMRYarnCluster.java` | REFERENCE | N/A | Reference for MiniMRCluster initialization |

**Common Workflow Test Files:**

| Target Test File | Transformation | Source/Reference | Purpose/Changes |
|-----------------|----------------|------------------|-----------------|
| `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/AbstractCommonWorkflowTest.java` | CREATE | `TestConfiguration.java` | Base class with Configuration fixtures |
| `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/TestConfigurationLoadingWorkflow.java` | CREATE | `org.apache.hadoop.conf.Configuration` | Tests for Workflow 14: addResource, get*, variable substitution, final values |
| `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/AbstractSecurityWorkflowTest.java` | CREATE | `TestUserGroupInformation.java` | Base class with static MiniKDC lifecycle |
| `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/TestKerberosAuthenticationWorkflow.java` | CREATE | `org.apache.hadoop.security.UserGroupInformation` | Tests for Workflow 15: loginUserFromKeytab, doAs, token renewal |
| `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/TestConfiguration.java` | REFERENCE | N/A | Reference for Configuration test patterns |
| `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/TestUserGroupInformation.java` | REFERENCE | N/A | Reference for UGI test patterns |

### 0.5.2 New Test Files Detail

**HDFS Workflow Tests:**

| File | Coverage | Test Methods |
|------|----------|--------------|
| `TestFileCreateWriteWorkflow.java` | Workflow 1 | `testCreateWriteVerify()`, `testParameterizedWriteSizes()`, `testOverwriteExisting()`, `testZeroByteFile()` |
| `TestFileReadWorkflow.java` | Workflow 2 | `testFullRead()`, `testParameterizedSeekRead()`, `testPositionalRead()`, `testReadBeyondEOF()` |
| `TestFileDeleteWorkflow.java` | Workflow 3 | `testDeleteSingleFile()`, `testDeleteRecursive()`, `testDeleteNonExistent()`, `testDeleteNonEmptyNonRecursive()` |
| `TestDirectoryOperationsWorkflow.java` | Workflow 4 | `testMkdirsListRename()`, `testMkdirsIdempotent()`, `testRenameToExisting()`, `testListEmptyDirectory()` |
| `TestPermissionAclWorkflow.java` | Workflow 5 | `testSetPermission()`, `testModifyAclEntries()`, `testSetAcl()`, `testRemoveAllAcls()`, `testAccessCheck()` |

**YARN Workflow Tests:**

| File | Coverage | Test Methods |
|------|----------|--------------|
| `TestApplicationSubmissionWorkflow.java` | Workflow 6 | `testSubmissionToRunning()`, `testInvalidResourceRequest()`, `testDuplicateSubmission()` |
| `TestResourceAllocationWorkflow.java` | Workflow 7 | `testContainerAllocation()`, `testImpossibleResourceRequest()`, `testLocalityRelaxation()` |
| `TestApplicationCompletionWorkflow.java` | Workflow 8 | `testSuccessfulCompletion()`, `testFailedCompletion()`, `testKillApplication()`, `testResourceRelease()` |
| `TestApplicationStatusWorkflow.java` | Workflow 9 | `testStatusQuery()`, `testProgressTracking()`, `testQueryNonExistent()`, `testDiagnosticsOnFailure()` |

**MapReduce Workflow Tests:**

| File | Coverage | Test Methods |
|------|----------|--------------|
| `TestJobSubmissionWorkflow.java` | Workflow 10 | `testJobSubmit()`, `testMissingInputPath()`, `testInvalidConfiguration()` |
| `TestMapPhaseWorkflow.java` | Workflow 11 | `testMapPhaseCompletion()`, `testMapTaskFailureRetry()`, `testMapCounters()` |
| `TestReducePhaseWorkflow.java` | Workflow 12 | `testReducePhaseCompletion()`, `testMapOnlyJob()`, `testCombinerVerification()`, `testOutputValidation()` |
| `TestJobFailureWorkflow.java` | Workflow 13 | `testJobFailureHandling()`, `testPartialMapFailure()`, `testDiagnosticsPopulated()`, `testOutputCleanup()` |

**Common Workflow Tests:**

| File | Coverage | Test Methods |
|------|----------|--------------|
| `TestConfigurationLoadingWorkflow.java` | Workflow 14 | `testXmlResourceLoading()`, `testProgrammaticOverride()`, `testVariableSubstitution()`, `testFinalProperty()`, `testMissingPropertyDefault()` |
| `TestKerberosAuthenticationWorkflow.java` | Workflow 15 | `testLoginFromKeytab()`, `testDoAsPrivilegedAction()`, `testTokenRenewal()`, `testExpiredTicket()`, `testInvalidKeytab()` |

### 0.5.3 Test Configuration Updates

No existing test configuration files require modification. New test classes will be automatically discovered by Maven Surefire via standard naming conventions (`Test*.java`).

**Surefire Discovery:**
- All `Test*Workflow.java` files in `workflow/` directories will be picked up by default include pattern
- No changes to `pom.xml` or Surefire configuration needed

### 0.5.4 Cross-File Test Dependencies

**Shared Fixtures:**

| Fixture Type | Location | Usage |
|--------------|----------|-------|
| Abstract base classes | `Abstract*WorkflowTest.java` in each workflow directory | Extended by all workflow tests |
| GenericTestUtils | `org.apache.hadoop.test.GenericTestUtils` | Imported for `waitFor()` |
| DFSTestUtil | `org.apache.hadoop.hdfs.DFSTestUtil` | Imported for HDFS helpers (optional) |

**Import Dependencies:**

All new test files will import:
- `org.junit.jupiter.api.*` - JUnit 5 annotations
- `org.apache.hadoop.test.GenericTestUtils` - Async wait utilities
- Component-specific classes (`HdfsConfiguration`, `YarnConfiguration`, etc.)

No import updates are required for existing files.

## 0.6 Dependency Inventory

### 0.6.1 Testing Dependencies

All testing packages are already configured in the Hadoop repository's `hadoop-project/pom.xml`. The following table lists key dependencies relevant to this testing exercise with their exact versions from the repository:

| Registry | Package Name | Version | Purpose |
|----------|--------------|---------|---------|
| Maven Central | org.junit.jupiter:junit-jupiter | 5.13.3 | JUnit 5 testing framework aggregate |
| Maven Central | org.junit.jupiter:junit-jupiter-api | 5.13.3 | JUnit 5 annotations and assertions |
| Maven Central | org.junit.jupiter:junit-jupiter-params | 5.13.3 | Parameterized test support |
| Maven Central | org.junit.jupiter:junit-jupiter-engine | 5.13.3 | JUnit 5 test engine |
| Maven Central | org.junit.platform:junit-platform-launcher | 1.13.3 | Test platform launcher |
| Maven Central | org.mockito:mockito-core | 4.11.0 | Mocking framework for unit tests |
| Maven Central | org.assertj:assertj-core | 3.12.2 | Fluent assertion library |
| Maven Central | org.hamcrest:hamcrest | 2.2 | Matcher library for assertions |

**MiniCluster Dependencies (Test Scope):**

| Registry | Package Name | Module | Purpose |
|----------|--------------|--------|---------|
| Maven Central | org.apache.hadoop:hadoop-hdfs | hadoop-hdfs-project/hadoop-hdfs | MiniDFSCluster |
| Maven Central | org.apache.hadoop:hadoop-yarn-server-tests | hadoop-yarn-project/hadoop-yarn-server-tests | MiniYARNCluster |
| Maven Central | org.apache.hadoop:hadoop-mapreduce-client-jobclient | hadoop-mapreduce-project | MiniMRYarnCluster |
| Maven Central | org.apache.hadoop:hadoop-minikdc | hadoop-common-project/hadoop-minikdc | MiniKDC |

**Test Utility Dependencies:**

| Registry | Package Name | Module | Purpose |
|----------|--------------|--------|---------|
| Maven Central | org.apache.hadoop:hadoop-common | hadoop-common-project/hadoop-common (test-jar) | GenericTestUtils, LambdaTestUtils |

### 0.6.2 Import Statements for New Test Classes

**HDFS Workflow Tests Common Imports:**

```java
// JUnit 5
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

// Hadoop Test Utilities
import org.apache.hadoop.test.GenericTestUtils;

// HDFS
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;

// Assertions
import static org.junit.jupiter.api.Assertions.*;
```

**YARN Workflow Tests Common Imports:**

```java
// JUnit 5
import org.junit.jupiter.api.*;

// Hadoop Test Utilities
import org.apache.hadoop.test.GenericTestUtils;

// YARN
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.api.records.*;

// Assertions
import static org.junit.jupiter.api.Assertions.*;
```

**MapReduce Workflow Tests Common Imports:**

```java
// JUnit 5
import org.junit.jupiter.api.*;

// Hadoop Test Utilities
import org.apache.hadoop.test.GenericTestUtils;

// MapReduce
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.*;

// Assertions
import static org.junit.jupiter.api.Assertions.*;
```

**Security Workflow Tests Common Imports:**

```java
// JUnit 5
import org.junit.jupiter.api.*;

// Hadoop Test Utilities
import org.apache.hadoop.test.GenericTestUtils;

// Security
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.FileUtil;

// Assertions
import static org.junit.jupiter.api.Assertions.*;
```

### 0.6.3 No Additional Dependencies Required

All required dependencies for the workflow test suite are already present in the Hadoop repository. No new dependencies need to be added to any `pom.xml` file.

**Verification Checklist:**
- ✅ JUnit Jupiter 5.13.3 - Configured in `hadoop-project/pom.xml`
- ✅ Mockito 4.11.0 - Configured in `hadoop-project/pom.xml`
- ✅ AssertJ 3.12.2 - Configured in `hadoop-project/pom.xml`
- ✅ MiniDFSCluster - Available in `hadoop-hdfs` test scope
- ✅ MiniYARNCluster - Available in `hadoop-yarn-server-tests`
- ✅ MiniMRYarnCluster - Available in `hadoop-mapreduce-client-jobclient`
- ✅ MiniKDC - Available in `hadoop-minikdc`
- ✅ GenericTestUtils - Available in `hadoop-common` test-jar

## 0.7 Coverage and Quality Targets

### 0.7.1 Coverage Metrics

**Current Coverage:** Not explicitly measured for targeted packages (baseline to be established)

**Target Coverage:** 80% line coverage on targeted packages

**Coverage Gaps to Address:**

| Package | Target Coverage | Focus Areas |
|---------|----------------|-------------|
| `org.apache.hadoop.hdfs` | 80% | FileSystem create/write/read/delete operations, block handling |
| `org.apache.hadoop.fs` | 80% | FileSystem API methods, FSDataInputStream, FSDataOutputStream |
| `org.apache.hadoop.yarn.client` | 80% | YarnClient application lifecycle, AMRMClient allocation |
| `org.apache.hadoop.yarn.api` | 80% | Resource request/response, ApplicationReport |
| `org.apache.hadoop.mapreduce` | 80% | Job submission, counters, task reporting |
| `org.apache.hadoop.conf` | 80% | Configuration loading, variable substitution, final values |
| `org.apache.hadoop.security` | 80% | UserGroupInformation login, doAs, token handling |

**Per-Workflow Coverage Targets:**

| Workflow ID | Workflow Name | Coverage Target | Critical Paths |
|-------------|---------------|-----------------|----------------|
| 1 | File Create/Write/Close | 100% happy path, 80% edge cases | `create()`, `write()`, `close()` |
| 2 | File Read | 100% happy path, 80% edge cases | `open()`, `read()`, `seek()`, `readFully()` |
| 3 | File Delete | 100% happy path, 80% edge cases | `delete()`, `exists()` |
| 4 | Directory Operations | 100% happy path, 80% edge cases | `mkdirs()`, `listStatus()`, `rename()` |
| 5 | Permission/ACL | 100% happy path, 80% edge cases | `setPermission()`, `setAcl()`, `access()` |
| 6 | Application Submission | 100% happy path, 80% edge cases | `createApplication()`, `submitApplication()` |
| 7 | Resource Allocation | 100% happy path, 80% edge cases | `addContainerRequest()`, `allocate()` |
| 8 | Application Completion | 100% happy path, 80% edge cases | `unregisterApplicationMaster()`, `killApplication()` |
| 9 | Application Status | 100% happy path, 80% edge cases | `getApplicationReport()` |
| 10 | Job Submission | 100% happy path, 80% edge cases | `submit()`, `getJobState()` |
| 11 | Map Phase | 100% happy path, 80% edge cases | `getCounters()`, `getTaskReports()` |
| 12 | Reduce Phase | 100% happy path, 80% edge cases | Output validation, combiner verification |
| 13 | Job Failure | 100% happy path, 80% edge cases | Failure handling, diagnostics |
| 14 | Configuration Loading | 100% happy path, 80% edge cases | `addResource()`, `get*()` methods |
| 15 | Kerberos Auth | 100% happy path, 80% edge cases | `loginUserFromKeytab()`, `doAs()` |

### 0.7.2 Test Quality Criteria

**Assertion Density Expectations:**

| Test Type | Minimum Assertions | Typical Range |
|-----------|-------------------|---------------|
| Happy path | 3 | 3-5 assertions |
| Edge case | 2 | 2-4 assertions |
| Error case | 2 | 2-3 assertions |
| Parameterized | 2 per iteration | 2-4 per iteration |

**Test Isolation Requirements:**

- Each test method must be independently executable
- No test may depend on the execution order of other tests
- Test namespace directories are created in `@BeforeEach` and deleted in `@AfterEach`
- No shared mutable state between test methods (cluster state is reset via namespace cleanup)

**Performance Constraints:**

| Metric | Target | Enforcement |
|--------|--------|-------------|
| Single test execution | ≤60 seconds | `@Timeout(value = 60, unit = TimeUnit.SECONDS)` |
| Full suite execution | ≤45 minutes | Static cluster reuse, parameterized tests |
| Cluster init overhead | ~4 minutes | Static `@BeforeAll` in abstract base classes |
| Average test time | ~10 seconds | Efficient test design, no unnecessary waits |

**Maintainability Standards:**

| Standard | Requirement |
|----------|-------------|
| Javadoc | Each test method documents workflow path and production APIs invoked |
| Naming | Test methods follow `test[Scenario]()` convention |
| Single responsibility | Each test validates one workflow path |
| DRY principle | Common setup in abstract base classes, no duplicate test logic |
| Parameterization | Use `@ParameterizedTest` for scenarios differing only by input values |

**Following Repository Test Patterns:**

| Pattern | Source | Application |
|---------|--------|-------------|
| Static cluster lifecycle | `TestMiniDFSCluster.java` | Abstract base classes use `@BeforeAll`/`@AfterAll` |
| GenericTestUtils.waitFor() | `TestDFSInputStream.java` | All async state transitions in YARN/MR tests |
| Test namespace isolation | `TestFileStatus.java` | `/workflow/[className]/[methodName]/` pattern |
| Parameterized tests | `TestConfiguration.java` | `@MethodSource` for input variations |

### 0.7.3 Redundancy Validation

**Non-Redundancy Requirement:** No two tests may share identical (input conditions → operation → validation) tuple.

**Redundancy Check Matrix (Example for HDFS Write):**

| Test | Input Conditions | Operation | Validation | Unique? |
|------|-----------------|-----------|------------|---------|
| testZeroByteWrite | writeSize=0, repl=2 | create→write→close | len=0, exists=true | ✅ |
| testSubBlockWrite | writeSize=1KB, repl=2 | create→write→close | len=1024, content match | ✅ |
| testBlockBoundaryWrite | writeSize=128MB, repl=3 | create→write→close | len=128MB, repl=3 | ✅ |
| testMultiBlockWrite | writeSize=128MB+1, repl=2 | create→write→close | len>blockSize, multi-block | ✅ |

**Validation Gate:** Each test method Javadoc must specify unique (input, operation, validation) triple to pass review.

## 0.8 Scope Boundaries

### 0.8.1 Exhaustively In Scope

**New Test Files (with trailing patterns):**

| Category | Pattern | Description |
|----------|---------|-------------|
| HDFS Abstract Base | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/Abstract*.java` | HDFS workflow base classes |
| HDFS Workflow Tests | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/Test*Workflow.java` | All HDFS workflow tests |
| YARN Abstract Base | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/Abstract*.java` | YARN workflow base classes |
| YARN Workflow Tests | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/Test*Workflow.java` | All YARN workflow tests |
| MR Abstract Base | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/Abstract*.java` | MapReduce workflow base classes |
| MR Workflow Tests | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/Test*Workflow.java` | All MapReduce workflow tests |
| Common Config Base | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/Abstract*.java` | Configuration workflow base classes |
| Common Config Tests | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/Test*Workflow.java` | Configuration workflow tests |
| Security Base | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/Abstract*.java` | Security workflow base classes |
| Security Tests | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/Test*Workflow.java` | Security workflow tests |

**Complete File Inventory (20 Files):**

| # | File Path | Type |
|---|-----------|------|
| 1 | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/AbstractHdfsWorkflowTest.java` | Abstract Base |
| 2 | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestFileCreateWriteWorkflow.java` | Test Class |
| 3 | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestFileReadWorkflow.java` | Test Class |
| 4 | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestFileDeleteWorkflow.java` | Test Class |
| 5 | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestDirectoryOperationsWorkflow.java` | Test Class |
| 6 | `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/workflow/TestPermissionAclWorkflow.java` | Test Class |
| 7 | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/AbstractYarnWorkflowTest.java` | Abstract Base |
| 8 | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestApplicationSubmissionWorkflow.java` | Test Class |
| 9 | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestResourceAllocationWorkflow.java` | Test Class |
| 10 | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestApplicationCompletionWorkflow.java` | Test Class |
| 11 | `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/workflow/TestApplicationStatusWorkflow.java` | Test Class |
| 12 | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/AbstractMapReduceWorkflowTest.java` | Abstract Base |
| 13 | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestJobSubmissionWorkflow.java` | Test Class |
| 14 | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestMapPhaseWorkflow.java` | Test Class |
| 15 | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestReducePhaseWorkflow.java` | Test Class |
| 16 | `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/workflow/TestJobFailureWorkflow.java` | Test Class |
| 17 | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/AbstractCommonWorkflowTest.java` | Abstract Base |
| 18 | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/workflow/TestConfigurationLoadingWorkflow.java` | Test Class |
| 19 | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/AbstractSecurityWorkflowTest.java` | Abstract Base |
| 20 | `hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/workflow/TestKerberosAuthenticationWorkflow.java` | Test Class |

### 0.8.2 Explicitly Out of Scope

**Must NOT Modify:**

| Category | Pattern/File | Reason |
|----------|--------------|--------|
| Existing test classes | `**/src/test/java/**/*.java` (outside workflow/) | Minimal change principle |
| Test resources | `**/src/test/resources/**` | Used by existing tests |
| CI pipeline configs | `Jenkinsfile`, `.github/workflows/**` | Infrastructure stability |
| Test utilities | `org.apache.hadoop.test.*` | Shared test infrastructure |
| MiniCluster implementations | `MiniDFSCluster.java`, `MiniYARNCluster.java`, etc. | Core test infrastructure |
| Production source code | `**/src/main/java/**` | Test-only changes |
| Build configurations | `**/pom.xml` | No dependency changes needed |

**Explicitly Excluded by User Instructions:**

| Exclusion | Rationale |
|-----------|-----------|
| Source code modifications | "DO NOT modify source code unless absolutely necessary" |
| Production refactoring | "Do not refactor production code for testability" |
| Feature additions | Adding tests only, not features |
| Unrelated test files | Only workflow/ directory tests in scope |
| Performance optimizations | Not related to test coverage |
| Existing test modifications | "Do not modify existing test classes" |

### 0.8.3 Boundary Validation Rules

**Inclusion Criteria:**
- File is a new test class in a `workflow/` directory
- File is a new abstract base class for workflow tests
- File follows the `Test*Workflow.java` or `Abstract*WorkflowTest.java` naming convention

**Exclusion Criteria:**
- Any modification to existing files
- Any file outside designated `workflow/` directories
- Any production source code changes
- Any build configuration changes
- Any CI/CD pipeline changes

## 0.9 Execution Parameters

### 0.9.1 Testing-Specific Instructions

**Test Execution Commands:**

| Purpose | Command |
|---------|---------|
| Run all workflow tests | `mvn test -Dtest=**/workflow/**` |
| Run HDFS workflow tests | `mvn test -pl hadoop-hdfs-project/hadoop-hdfs -Dtest=**/workflow/**` |
| Run YARN workflow tests | `mvn test -pl hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client -Dtest=**/workflow/**` |
| Run MR workflow tests | `mvn test -pl hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient -Dtest=**/workflow/**` |
| Run Common workflow tests | `mvn test -pl hadoop-common-project/hadoop-common -Dtest=**/workflow/**` |

**Coverage Measurement Commands:**

```bash
# Generate coverage report for HDFS targeted packages

mvn test -pl hadoop-hdfs-project/hadoop-hdfs \
    -Dtest=**/workflow/** \
    -Djacoco.includes=org/apache/hadoop/hdfs/**:org/apache/hadoop/fs/** \
    jacoco:report

#### Generate coverage report for YARN targeted packages

mvn test -pl hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client \
    -Dtest=**/workflow/** \
    -Djacoco.includes=org/apache/hadoop/yarn/client/**:org/apache/hadoop/yarn/api/** \
    jacoco:report
```

**Single Test Execution Pattern:**

```bash
# Run a specific test class

mvn test -Dtest=TestFileCreateWriteWorkflow

#### Run a specific test method

mvn test -Dtest=TestFileCreateWriteWorkflow#testCreateWriteVerify
```

**Debug Mode Execution:**

```bash
# Run with debug logging

mvn test -Dtest=**/workflow/** -Dhadoop.root.logger=DEBUG,console

#### Run with remote debugging

mvn test -Dtest=**/workflow/** \
    -Dmaven.surefire.debug="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
```

### 0.9.2 Test Patterns to Follow

**Existing Repository Patterns:**

| Pattern | Location | Description |
|---------|----------|-------------|
| Static MiniDFSCluster | `TestMiniDFSCluster.java` | `@BeforeAll` init, `@AfterAll` shutdown |
| GenericTestUtils.waitFor | `TestDFSInputStream.java` | Async condition waiting |
| Parameterized tests | `TestConfiguration.java` | `@ParameterizedTest` with `@MethodSource` |
| Test isolation | `TestFileStatus.java` | Unique directories per test |

**Async Wait Pattern (MANDATORY for YARN/MR):**

```java
// Correct: Use GenericTestUtils.waitFor()
GenericTestUtils.waitFor(
    () -> {
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        return report.getYarnApplicationState() == YarnApplicationState.RUNNING;
    },
    500,    // check interval ms
    60000   // timeout ms
);

// FORBIDDEN: Thread.sleep()
// Thread.sleep(10000);  // Never use this
```

**Parameterized Test Pattern (MANDATORY for coverage efficiency):**

```java
@ParameterizedTest(name = "writeSize={0}, replication={1}")
@MethodSource("writeParameters")
void testFileWriteWorkflow(int writeSize, short replication) {
    // Test implementation
}

static Stream<Arguments> writeParameters() {
    return Stream.of(
        Arguments.of(0, (short) 2),           // zero-byte
        Arguments.of(1024, (short) 2),        // sub-block
        Arguments.of(128 * 1024 * 1024, (short) 3)  // block boundary
    );
}
```

### 0.9.3 Environment Setup Requirements

**Java Version:**
- Java 17 (as specified in `hadoop-project/pom.xml`)
- Verify: `java -version` should show 17.x

**Maven Version:**
- Maven 3.3.0 or later
- Verify: `mvn -version` should show 3.3+

**Environment Variables:**

| Variable | Purpose | Typical Value |
|----------|---------|---------------|
| `JAVA_HOME` | Java installation | `/usr/lib/jvm/java-17-openjdk` |
| `MAVEN_OPTS` | Maven JVM options | `-Xmx4g -XX:+UseG1GC` |
| `test.build.dir` | Test output directory | `target/test-dir` |

**Test Execution Environment:**

```bash
# Ensure sufficient memory for MiniCluster tests

export MAVEN_OPTS="-Xmx4g -XX:+UseG1GC"

#### Run tests with single fork for cluster reuse

mvn test -pl hadoop-hdfs-project/hadoop-hdfs \
    -Dtest=**/workflow/** \
    -DforkCount=1
```

### 0.9.4 Excluded Test Categories

Per user instruction, the following are NOT in scope:

| Category | Reason |
|----------|--------|
| Performance tests | Not related to workflow coverage |
| Stress tests | Outside scope of workflow validation |
| Compatibility tests | Focus is on functional workflows |
| Upgrade tests | Not specified in requirements |

### 0.9.5 Test Count Validation

```bash
# Count test methods in workflow tests

find . -path "*/workflow/*.java" -exec grep -c "@Test\|@ParameterizedTest" {} + \
    | awk -F: '{sum+=$2} END {print "Total test methods:", sum}'

#### Target: ≤250 total tests

```

**Estimated Test Distribution:**

| Component | Test Classes | Est. Tests/Class | Total Tests |
|-----------|--------------|------------------|-------------|
| HDFS | 5 | 8-12 | ~50 |
| YARN | 4 | 6-10 | ~32 |
| MapReduce | 4 | 8-12 | ~40 |
| Common | 2 | 8-15 | ~23 |
| **Total** | **15** | - | **~145** |

Note: Parameterized tests count as multiple test executions but single test methods.

## 0.10 Special Instructions for Testing

### 0.10.1 User-Specified Testing Directives

The following directives have been explicitly emphasized by the user and MUST be adhered to throughout implementation:

**Minimal Change Principle:**
- "ONLY modify test files and test-related configurations"
- "DO NOT modify source code unless absolutely necessary for testability"
- Add only test files in designated `workflow/` directories
- Do not modify existing test classes or test utilities
- Do not refactor production code for testability
- Do not modify existing test configurations or CI pipelines

**Production Function Exclusivity:**
- All tests MUST invoke production functions directly—zero tolerance for reimplemented logic
- Mock/stub ONLY external dependencies—never internal functions under test
- Test assertions validate return values and side effects of actual production code paths
- No inline recreation of algorithms, calculations, or transformations that exist in source

**Static Cluster Reuse:**
- All test classes MUST extend the appropriate `Abstract*WorkflowTest` base class
- MiniCluster initialization costs 15-30 seconds—static reuse is required to meet 45-minute budget

**Async Pattern Compliance:**
- YARN/MR tests MUST use `GenericTestUtils.waitFor()` for all async state transitions
- Zero `Thread.sleep()` calls in test bodies
- Use `@Timeout` annotation for per-test time limits

**Parameterization Requirement:**
- Use `@ParameterizedTest` for scenarios differing only by input values
- Avoid duplicate test methods that differ only by parameter values

### 0.10.2 Forbidden Patterns

| Pattern | Example | Why Forbidden | Required Alternative |
|---------|---------|---------------|---------------------|
| `Thread.sleep(n)` | `Thread.sleep(5000);` | Non-deterministic, wastes time | `GenericTestUtils.waitFor()` |
| Busy-wait polling | `while (!condition) { ... }` | CPU waste, timing variance | `GenericTestUtils.waitFor()` |
| Logic copying | Reimplementing checksum in test | Tests the copy, not production | Invoke production method |
| Mock-testing | Asserting mock return values only | Zero production coverage | Test actual implementation |
| Hardcoded bypass | `assertEquals("abc123", hash)` | Validates constant, not function | Compute via production code |
| Internal mocking | `when(internalMethod()).thenReturn()` | Defeats test purpose | Test via public API |

### 0.10.3 Compliant Test Structure

**Required Test Method Structure:**

```java
/**
 * Workflow path: [Describe workflow]
 * Production methods invoked: [List production APIs]
 * Input conditions: [Describe unique inputs]
 * Validation criteria: [Describe assertions]
 */
@Test
@Timeout(value = 60, unit = TimeUnit.SECONDS)
void testWorkflowScenario() throws Exception {
    // ARRANGE: Setup via production APIs or fixtures
    Path testPath = new Path(testDir, "testfile.txt");
    
    // ACT: Invoke production functions under test
    try (FSDataOutputStream out = fs.create(testPath)) {  // ← PRODUCTION
        out.write(testData);                               // ← PRODUCTION
    }
    
    // ASSERT: Validate via production queries
    assertTrue(fs.exists(testPath));                       // ← PRODUCTION
    FileStatus status = fs.getFileStatus(testPath);        // ← PRODUCTION
    assertEquals(testData.length, status.getLen());
}
```

### 0.10.4 Test File Composition Rules

**Each test file must contain exactly these categories of code:**

| Category | Allowed | Examples |
|----------|---------|----------|
| Imports | ✅ Yes | `import org.apache.hadoop.fs.FileSystem;` |
| Test fixtures | ✅ Yes | `@BeforeAll`, `@BeforeEach`, `@AfterEach`, `@AfterAll` |
| Production API invocations | ✅ Yes | `fs.create()`, `fs.delete()`, `job.submit()` |
| Assertions | ✅ Yes | `assertEquals()`, `assertTrue()`, `assertThrows()` |
| Test data constants | ✅ Yes | `byte[] testData = new byte[1024];` |
| Business logic | ❌ No | Algorithms, calculations, transformations |
| Helper methods with logic | ❌ No | Methods that compute/transform beyond setup |

### 0.10.5 Mocking Boundaries

| Layer | Mock Allowed | Rationale |
|-------|--------------|-----------|
| External APIs (HTTP, RPC) | ✅ Yes | Network isolation via MiniCluster |
| Databases | ✅ Yes | State isolation via MiniCluster |
| File systems | ✅ Yes | MiniDFSCluster provides controlled environment |
| Hadoop internal classes | ❌ No | Must test actual implementation |
| Classes under test | ❌ No | Defeats purpose of test |
| Utility functions in source | ❌ No | Production path required |

### 0.10.6 Documentation Requirements

**Each test method Javadoc must include:**

1. **Workflow path:** Description of the workflow being tested
2. **Production methods invoked:** List of production APIs called
3. **Input conditions:** Unique input combination for this test
4. **Validation criteria:** What assertions verify

**Example:**

```java
/**
 * Workflow: File Create/Write/Close with block boundary
 * Production methods invoked: FileSystem.create(), FSDataOutputStream.write(),
 *                             FSDataOutputStream.close(), FileSystem.getFileStatus()
 * Input conditions: writeSize=128MB (exact block size), replication=3
 * Validation criteria: File length equals 128MB, replication factor equals 3,
 *                      content matches via read-back verification
 */
```

### 0.10.7 Validation Gates

All tests must pass the following validation gates before acceptance:

| Gate | Criteria | Blocking |
|------|----------|----------|
| Compilation | All new tests compile against trunk | Yes |
| Unit pass | 100% unit tests pass | Yes |
| Integration pass | 100% integration tests pass with MiniCluster | Yes |
| Coverage | 80% line coverage on targeted packages | Yes |
| Workflow completion | All 15 workflows exercised | Yes |
| Execution time | Full suite ≤45 minutes | Yes |
| Test count | ≤250 total tests | Yes |
| Redundancy | Zero duplicate workflow paths | Yes |
| Isolation | No cross-test failures when run individually | Yes |
| CI integration | Existing CI remains green | Yes |
| Production exclusivity | Zero reimplemented business logic | Yes |
| Mock boundaries | Only external dependencies mocked | Yes |
| Static cluster reuse | All test classes extend Abstract*WorkflowTest | Yes |
| Async patterns | YARN/MR tests use GenericTestUtils.waitFor() | Yes |
| Parameterization | Variable input scenarios use @ParameterizedTest | Yes |

### 0.10.8 Reference Documentation

Consult these resources during implementation:

| Domain | Resource | Purpose |
|--------|----------|---------|
| Security | Hadoop Security Guide | MiniKDC configuration matching Kerberos requirements |
| YARN | YARN Application Master Guide | Production sequence for ApplicationSubmissionContext |
| Testing | GenericTestUtils Javadoc | Correct `waitFor()` patterns for async transitions |
| HDFS | HDFS Architecture Guide | Block placement and replication behavior |
| JUnit 5 | JUnit 5 User Guide | `@ParameterizedTest`, `@Timeout`, lifecycle annotations |