/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.workflow;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY;

/**
 * Abstract base class for all HDFS workflow tests.
 * 
 * <p>This class provides static MiniDFSCluster lifecycle management to enable
 * efficient test execution by reusing the cluster across all test methods within
 * subclasses. The cluster initialization overhead (~10 seconds) is incurred only
 * once per test class, helping meet the 45-minute CI execution budget.
 * 
 * <h3>Lifecycle Management</h3>
 * <ul>
 *   <li>{@code @BeforeAll}: Initializes HdfsConfiguration with ACLs enabled,
 *       creates a MiniDFSCluster with 3 DataNodes, waits for cluster to become
 *       active, and obtains the DistributedFileSystem instance.</li>
 *   <li>{@code @AfterAll}: Closes the FileSystem and shuts down the cluster
 *       cleanly to release resources.</li>
 *   <li>{@code @BeforeEach}: Creates a unique test directory under the
 *       /workflow namespace using the pattern /workflow/[className]/[methodName]
 *       to ensure test isolation.</li>
 *   <li>{@code @AfterEach}: Recursively deletes the test directory to clean up
 *       any files created during the test.</li>
 * </ul>
 * 
 * <h3>Protected Fields Available to Subclasses</h3>
 * <ul>
 *   <li>{@code cluster} - The MiniDFSCluster instance for advanced cluster operations</li>
 *   <li>{@code conf} - The HdfsConfiguration used to configure the cluster</li>
 *   <li>{@code fs} - The DistributedFileSystem for file operations</li>
 *   <li>{@code testDir} - A unique Path for each test method under /workflow namespace</li>
 * </ul>
 * 
 * <h3>Configuration</h3>
 * <p>ACLs are enabled by default via {@code DFS_NAMENODE_ACLS_ENABLED_KEY} to support
 * permission and ACL workflow tests.
 * 
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class TestFileCreateWriteWorkflow extends AbstractHdfsWorkflowTest {
 *     
 *     @Test
 *     void testCreateFile() throws Exception {
 *         Path filePath = new Path(testDir, "testfile.txt");
 *         try (FSDataOutputStream out = fs.create(filePath)) {
 *             out.write("Hello World".getBytes(StandardCharsets.UTF_8));
 *         }
 *         assertTrue(fs.exists(filePath));
 *     }
 * }
 * }</pre>
 * 
 * @see MiniDFSCluster
 * @see DistributedFileSystem
 */
public abstract class AbstractHdfsWorkflowTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsWorkflowTest.class);

    /**
     * Number of DataNodes to start in the MiniDFSCluster.
     * Using 3 DataNodes to support default replication factor testing.
     */
    protected static final int NUM_DATANODES = 3;

    /**
     * Root namespace for all workflow test directories.
     * All test directories are created under this path for easy identification and cleanup.
     */
    protected static final String WORKFLOW_ROOT = "/workflow";

    /**
     * The MiniDFSCluster instance providing an in-process HDFS cluster for testing.
     * Initialized once per test class in {@link #setUpClass()} and shut down in {@link #tearDownClass()}.
     */
    protected static MiniDFSCluster cluster;

    /**
     * The HdfsConfiguration used to configure the MiniDFSCluster.
     * Contains settings such as ACL enablement that apply to all tests.
     */
    protected static HdfsConfiguration conf;

    /**
     * The DistributedFileSystem instance for performing file operations.
     * Obtained from the cluster and shared across all test methods.
     */
    protected static DistributedFileSystem fs;

    /**
     * The unique test directory path for each test method.
     * Created in {@link #setUp(TestInfo)} using the pattern /workflow/[className]/[methodName].
     * Deleted recursively in {@link #tearDown()}.
     */
    protected Path testDir;

    /**
     * Initializes the MiniDFSCluster and related resources.
     * 
     * <p>This method performs the following setup:
     * <ol>
     *   <li>Creates a new HdfsConfiguration instance</li>
     *   <li>Enables ACLs via DFS_NAMENODE_ACLS_ENABLED_KEY for permission/ACL testing</li>
     *   <li>Builds a MiniDFSCluster with 3 DataNodes using the Builder pattern</li>
     *   <li>Waits for all DataNodes to register and become active</li>
     *   <li>Obtains the DistributedFileSystem for file operations</li>
     * </ol>
     * 
     * <p>The cluster initialization typically takes ~10 seconds. By using static
     * initialization, this overhead is incurred only once per test class.
     * 
     * @throws IOException if cluster initialization fails
     */
    @BeforeAll
    public static void setUpClass() throws IOException {
        LOG.info("Initializing MiniDFSCluster for HDFS workflow tests");

        // Create configuration with ACLs enabled for permission testing
        conf = new HdfsConfiguration();
        conf.setBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY, true);

        // Build the MiniDFSCluster with 3 DataNodes
        // Using Builder pattern as per Hadoop test conventions
        cluster = new MiniDFSCluster.Builder(conf)
                .numDataNodes(NUM_DATANODES)
                .build();

        // Wait for all DataNodes to register with the NameNode
        // This ensures the cluster is fully operational before tests run
        cluster.waitActive();

        // Obtain the DistributedFileSystem for file operations
        fs = cluster.getFileSystem();

        LOG.info("MiniDFSCluster initialized successfully with {} DataNodes", NUM_DATANODES);
    }

    /**
     * Shuts down the MiniDFSCluster and releases all resources.
     * 
     * <p>This method performs cleanup in the correct order:
     * <ol>
     *   <li>Closes the FileSystem to release client resources</li>
     *   <li>Shuts down the MiniDFSCluster to stop all NameNode and DataNode processes</li>
     * </ol>
     * 
     * <p>Null checks ensure safe cleanup even if initialization failed partially.
     * 
     * @throws IOException if FileSystem close fails
     */
    @AfterAll
    public static void tearDownClass() throws IOException {
        LOG.info("Shutting down MiniDFSCluster");

        // Close the FileSystem first to release client resources
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                LOG.warn("Error closing FileSystem", e);
            }
            fs = null;
        }

        // Shutdown the cluster to stop NameNode and DataNodes
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }

        // Clear configuration reference
        conf = null;

        LOG.info("MiniDFSCluster shutdown complete");
    }

    /**
     * Creates a unique test directory for each test method.
     * 
     * <p>The directory path follows the pattern: /workflow/[className]/[methodName]
     * where className is the simple name of the test class and methodName is the
     * display name of the current test method.
     * 
     * <p>This ensures complete isolation between test methods, allowing each test
     * to operate in its own namespace without interference from other tests.
     * 
     * @param testInfo JUnit 5 TestInfo providing access to test class and method metadata
     * @throws IOException if directory creation fails
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        // Build unique test directory path using class and method names
        String className = testInfo.getTestClass()
                .map(Class::getSimpleName)
                .orElse("UnknownClass");
        String methodName = testInfo.getTestMethod()
                .map(java.lang.reflect.Method::getName)
                .orElse(testInfo.getDisplayName());

        // Create the test directory path
        testDir = new Path(WORKFLOW_ROOT, className + "/" + methodName);

        // Create the directory in HDFS
        boolean created = fs.mkdirs(testDir);
        if (created) {
            LOG.debug("Created test directory: {}", testDir);
        } else {
            // Directory may already exist from a previous interrupted run
            LOG.debug("Test directory already exists or creation returned false: {}", testDir);
        }
    }

    /**
     * Recursively deletes the test directory to clean up test artifacts.
     * 
     * <p>This method ensures complete cleanup after each test by recursively
     * deleting the test directory and all its contents. This prevents test
     * pollution and disk space accumulation during test suite execution.
     * 
     * <p>Errors during cleanup are logged but do not fail the test, as the
     * test itself has already completed. The cluster teardown will ultimately
     * clean up all data.
     * 
     * @throws IOException if directory deletion encounters an error
     */
    @AfterEach
    public void tearDown() throws IOException {
        if (testDir != null && fs != null) {
            try {
                // Check if directory exists before attempting deletion
                if (fs.exists(testDir)) {
                    // Recursively delete the test directory and all contents
                    boolean deleted = fs.delete(testDir, true);
                    if (deleted) {
                        LOG.debug("Deleted test directory: {}", testDir);
                    } else {
                        LOG.warn("Failed to delete test directory: {}", testDir);
                    }
                }
            } catch (IOException e) {
                // Log warning but don't fail - test has already completed
                LOG.warn("Error cleaning up test directory: {}", testDir, e);
            }
        }
        testDir = null;
    }

    /**
     * Returns the MiniDFSCluster instance.
     * 
     * <p>Subclasses can use this method when they need direct access to cluster
     * operations such as restarting DataNodes or accessing internal cluster state.
     * 
     * @return the MiniDFSCluster instance
     */
    protected MiniDFSCluster getCluster() {
        return cluster;
    }

    /**
     * Returns the HdfsConfiguration instance.
     * 
     * <p>Subclasses can use this method to read configuration values or create
     * additional configuration-dependent objects.
     * 
     * @return the HdfsConfiguration instance
     */
    protected HdfsConfiguration getConf() {
        return conf;
    }

    /**
     * Returns the DistributedFileSystem instance.
     * 
     * <p>Subclasses can use this method to perform file operations. This is
     * the primary interface for HDFS workflow tests.
     * 
     * @return the DistributedFileSystem instance
     */
    protected DistributedFileSystem getFileSystem() {
        return fs;
    }

    /**
     * Returns the unique test directory path for the current test method.
     * 
     * <p>Subclasses should use this path as the base for all file and directory
     * operations to ensure proper test isolation.
     * 
     * @return the test directory Path
     */
    protected Path getTestDir() {
        return testDir;
    }
}
