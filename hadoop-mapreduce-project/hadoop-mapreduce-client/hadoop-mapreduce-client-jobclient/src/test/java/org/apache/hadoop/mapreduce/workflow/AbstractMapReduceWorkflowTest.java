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

package org.apache.hadoop.mapreduce.workflow;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for MapReduce workflow tests providing static
 * MiniMRYarnCluster and MiniDFSCluster lifecycle management using JUnit 5
 * {@code @BeforeAll}/{@code @AfterAll} annotations.
 *
 * <p>This class provides:
 * <ul>
 *   <li>Shared cluster instances (mrCluster, dfsCluster) that are reused
 *       across all test methods to meet 45-minute CI budget constraints</li>
 *   <li>FileSystem access (remoteFs for HDFS, localFs for local operations)</li>
 *   <li>Configuration (conf) pre-configured for MiniMRYarnCluster</li>
 *   <li>Per-test directory isolation via {@code @BeforeEach}/{@code @AfterEach}
 *       for test namespace management under /workflow/[className]/[methodName]/</li>
 * </ul>
 *
 * <p>Test classes extending this base class can focus on workflow validation
 * without cluster setup/teardown overhead in each test method.
 *
 * <p>Usage:
 * <pre>{@code
 * public class TestJobSubmissionWorkflow extends AbstractMapReduceWorkflowTest {
 *     @Test
 *     void testJobSubmit() throws Exception {
 *         // Use inherited fields: mrCluster, remoteFs, conf, testDir
 *         Job job = Job.getInstance(conf);
 *         // ... configure and submit job
 *     }
 * }
 * }</pre>
 *
 * @see MiniMRYarnCluster
 * @see MiniDFSCluster
 */
public abstract class AbstractMapReduceWorkflowTest {

    private static final Logger LOG = LoggerFactory.getLogger(
            AbstractMapReduceWorkflowTest.class);

    /**
     * Number of DataNodes to start in MiniDFSCluster.
     * Using 3 DataNodes to support default replication factor of 3.
     */
    private static final int NUM_DATA_NODES = 3;

    /**
     * Number of NodeManagers to start in MiniYARNCluster.
     * Using 3 NodeManagers for parallel task execution testing.
     */
    private static final int NUM_NODE_MANAGERS = 3;

    /**
     * Maximum cluster-level application priority for YARN.
     */
    private static final int MAX_APP_PRIORITY = 10;

    /**
     * Root directory for workflow tests in HDFS.
     */
    private static final String WORKFLOW_ROOT = "/workflow";

    /**
     * Static MiniMRYarnCluster instance shared across all test methods.
     * Initialized in {@link #setup()} and stopped in {@link #tearDown()}.
     */
    protected static MiniMRYarnCluster mrCluster;

    /**
     * Static MiniDFSCluster instance providing HDFS backend for MapReduce jobs.
     * Initialized in {@link #setup()} and shutdown in {@link #tearDown()}.
     */
    protected static MiniDFSCluster dfsCluster;

    /**
     * Configuration used by both clusters.
     * Pre-configured with HDFS as default filesystem and MR staging directory.
     */
    protected static Configuration conf;

    /**
     * FileSystem instance for HDFS operations.
     * Connected to the MiniDFSCluster.
     */
    protected static FileSystem remoteFs;

    /**
     * Local FileSystem instance for local file operations.
     * Used for copying MRAppJar and other local resources.
     */
    protected static FileSystem localFs;

    /**
     * Path to the MRAppJar in local filesystem.
     * Copied from MiniMRYarnCluster.APPJAR with private permissions.
     */
    protected static Path APP_JAR;

    /**
     * Per-test directory for test isolation.
     * Created in {@link #createTestDir(TestInfo)} and cleaned up in
     * {@link #cleanupTestDir()}.
     * Path pattern: /workflow/[TestClassName]/[methodName]/
     */
    protected Path testDir;

    /**
     * Local test root directory for temporary files.
     */
    private static Path localTestRoot;

    /**
     * Sets up the static MiniDFSCluster and MiniMRYarnCluster instances.
     * This method is called once before all test methods in any subclass.
     *
     * <p>Setup sequence:
     * <ol>
     *   <li>Initialize Configuration</li>
     *   <li>Start MiniDFSCluster with configured DataNodes</li>
     *   <li>Wait for cluster to become active</li>
     *   <li>Configure MiniMRYarnCluster with HDFS as default FS</li>
     *   <li>Start MiniMRYarnCluster</li>
     *   <li>Copy MRAppJar for test job submissions</li>
     * </ol>
     *
     * @throws IOException if cluster initialization fails
     */
    @BeforeAll
    public static void setup() throws IOException {
        LOG.info("Setting up MiniDFSCluster and MiniMRYarnCluster for workflow tests");

        // Initialize configuration
        conf = new Configuration();

        // Initialize local filesystem
        try {
            localFs = FileSystem.getLocal(conf);
        } catch (IOException e) {
            LOG.error("Failed to get local filesystem", e);
            throw new RuntimeException("Problem getting local filesystem", e);
        }

        // Setup local test root directory
        localTestRoot = localFs.makeQualified(
                new Path("target", AbstractMapReduceWorkflowTest.class.getName() + "-tmpDir"));
        APP_JAR = new Path(localTestRoot, "MRAppJar.jar");

        // Start MiniDFSCluster
        try {
            LOG.info("Starting MiniDFSCluster with {} DataNodes", NUM_DATA_NODES);
            dfsCluster = new MiniDFSCluster.Builder(conf)
                    .numDataNodes(NUM_DATA_NODES)
                    .format(true)
                    .build();
            dfsCluster.waitActive();
            remoteFs = dfsCluster.getFileSystem();
            LOG.info("MiniDFSCluster started successfully. HDFS URI: {}",
                    remoteFs.getUri());
        } catch (IOException e) {
            LOG.error("Failed to start MiniDFSCluster", e);
            throw new RuntimeException("Problem starting MiniDFSCluster", e);
        }

        // Check if MRAppJar exists before starting MR cluster
        if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
            LOG.warn("MRAppJar {} not found. Skipping MiniMRYarnCluster setup.",
                    MiniMRYarnCluster.APPJAR);
            return;
        }

        // Start MiniMRYarnCluster
        if (mrCluster == null) {
            LOG.info("Starting MiniMRYarnCluster with {} NodeManagers", NUM_NODE_MANAGERS);

            mrCluster = new MiniMRYarnCluster(
                    AbstractMapReduceWorkflowTest.class.getSimpleName(),
                    NUM_NODE_MANAGERS);

            // Configure MR cluster with HDFS as default filesystem
            Configuration mrConf = new Configuration();
            mrConf.set("fs.defaultFS", remoteFs.getUri().toString());
            mrConf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
            mrConf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
                    MAX_APP_PRIORITY);

            mrCluster.init(mrConf);
            mrCluster.start();

            // Update conf with cluster configuration
            conf = mrCluster.getConfig();

            LOG.info("MiniMRYarnCluster started successfully");
            LOG.info("ResourceManager address: {}",
                    conf.get(YarnConfiguration.RM_ADDRESS));
        }

        // Copy MRAppJar to local test directory with private permissions
        // This is required since public distributed cache is not available in test
        copyMRAppJar();

        // Create workflow root directory in HDFS
        createWorkflowRootDirectory();

        LOG.info("Cluster setup completed successfully");
    }

    /**
     * Copies the MRAppJar to the local test directory with private permissions.
     * This is a workaround for the absent public distributed cache in test env.
     *
     * @throws IOException if copy operation fails
     */
    private static void copyMRAppJar() throws IOException {
        LOG.info("Copying MRAppJar from {} to {}", MiniMRYarnCluster.APPJAR, APP_JAR);

        // Ensure parent directory exists
        Path parentDir = APP_JAR.getParent();
        if (!localFs.exists(parentDir)) {
            localFs.mkdirs(parentDir);
        }

        // Copy the jar file
        localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);

        // Set private permissions (700) to simulate private distributed cache
        localFs.setPermission(APP_JAR, new FsPermission("700"));

        LOG.info("MRAppJar copied successfully with private permissions");
    }

    /**
     * Creates the workflow root directory in HDFS if it doesn't exist.
     *
     * @throws IOException if directory creation fails
     */
    private static void createWorkflowRootDirectory() throws IOException {
        Path workflowRoot = new Path(WORKFLOW_ROOT);
        if (!remoteFs.exists(workflowRoot)) {
            remoteFs.mkdirs(workflowRoot);
            LOG.info("Created workflow root directory: {}", WORKFLOW_ROOT);
        }
    }

    /**
     * Tears down the static cluster instances.
     * This method is called once after all test methods in any subclass.
     *
     * <p>Teardown sequence:
     * <ol>
     *   <li>Stop MiniMRYarnCluster if running</li>
     *   <li>Shutdown MiniDFSCluster if running</li>
     *   <li>Cleanup local temporary directories</li>
     * </ol>
     *
     * @throws IOException if cluster shutdown fails
     */
    @AfterAll
    public static void tearDown() throws IOException {
        LOG.info("Tearing down MiniMRYarnCluster and MiniDFSCluster");

        // Stop MiniMRYarnCluster
        if (mrCluster != null) {
            LOG.info("Stopping MiniMRYarnCluster");
            mrCluster.stop();
            mrCluster = null;
        }

        // Shutdown MiniDFSCluster
        if (dfsCluster != null) {
            LOG.info("Shutting down MiniDFSCluster");
            dfsCluster.shutdown();
            dfsCluster = null;
        }

        // Cleanup local test resources
        cleanupLocalTestResources();

        LOG.info("Cluster teardown completed");
    }

    /**
     * Cleans up local test resources including the MRAppJar copy.
     *
     * @throws IOException if cleanup fails
     */
    private static void cleanupLocalTestResources() throws IOException {
        if (localFs != null && localTestRoot != null && localFs.exists(localTestRoot)) {
            LOG.info("Cleaning up local test resources: {}", localTestRoot);
            localFs.delete(localTestRoot, true);
        }
    }

    /**
     * Creates a unique test directory for the current test method.
     * Called before each test method to ensure test isolation.
     *
     * <p>Directory path pattern: /workflow/[TestClassName]/[testMethodName]/
     *
     * @param testInfo JUnit 5 TestInfo providing test method metadata
     * @throws IOException if directory creation fails
     */
    @BeforeEach
    public void createTestDir(TestInfo testInfo) throws IOException {
        // Get test class and method names for unique directory path
        String className = testInfo.getTestClass()
                .map(Class::getSimpleName)
                .orElse("UnknownClass");
        String methodName = testInfo.getTestMethod()
                .map(java.lang.reflect.Method::getName)
                .orElse(testInfo.getDisplayName());

        // Sanitize method name (remove special characters that might be invalid in paths)
        String sanitizedMethodName = methodName.replaceAll("[^a-zA-Z0-9_-]", "_");

        // Construct unique test directory path
        testDir = new Path(WORKFLOW_ROOT, className + "/" + sanitizedMethodName);

        // Create the test directory in HDFS
        if (remoteFs != null) {
            if (remoteFs.exists(testDir)) {
                // Clean up any leftover from previous run
                remoteFs.delete(testDir, true);
            }
            remoteFs.mkdirs(testDir);
            LOG.debug("Created test directory: {}", testDir);
        } else {
            LOG.warn("RemoteFs is null, skipping test directory creation");
        }
    }

    /**
     * Cleans up the test directory after each test method.
     * Called after each test method to ensure proper cleanup.
     *
     * @throws IOException if directory deletion fails
     */
    @AfterEach
    public void cleanupTestDir() throws IOException {
        if (remoteFs != null && testDir != null) {
            try {
                if (remoteFs.exists(testDir)) {
                    boolean deleted = remoteFs.delete(testDir, true);
                    if (deleted) {
                        LOG.debug("Deleted test directory: {}", testDir);
                    } else {
                        LOG.warn("Failed to delete test directory: {}", testDir);
                    }
                }
            } catch (IOException e) {
                LOG.warn("Exception while cleaning up test directory: {}", testDir, e);
                // Don't rethrow - cleanup failures shouldn't fail the test
            }
        }
        testDir = null;
    }

    /**
     * Returns the current test directory path.
     * Useful for subclasses that need to reference the test directory.
     *
     * @return the test directory Path, or null if not set
     */
    protected Path getTestDir() {
        return testDir;
    }

    /**
     * Returns the MiniMRYarnCluster instance.
     * Useful for subclasses that need direct cluster access.
     *
     * @return the MiniMRYarnCluster instance, or null if not started
     */
    protected MiniMRYarnCluster getMrCluster() {
        return mrCluster;
    }

    /**
     * Returns the MiniDFSCluster instance.
     * Useful for subclasses that need direct HDFS cluster access.
     *
     * @return the MiniDFSCluster instance, or null if not started
     */
    protected MiniDFSCluster getDfsCluster() {
        return dfsCluster;
    }

    /**
     * Returns the cluster configuration.
     * The configuration is pre-configured with HDFS as default filesystem
     * and appropriate MR settings.
     *
     * @return the Configuration instance
     */
    protected Configuration getConf() {
        return conf;
    }

    /**
     * Returns the HDFS FileSystem instance.
     *
     * @return the remote FileSystem connected to MiniDFSCluster
     */
    protected FileSystem getRemoteFs() {
        return remoteFs;
    }

    /**
     * Returns the local FileSystem instance.
     *
     * @return the local FileSystem
     */
    protected FileSystem getLocalFs() {
        return localFs;
    }

    /**
     * Returns the path to the MRAppJar.
     *
     * @return the Path to the MRAppJar
     */
    protected Path getAppJar() {
        return APP_JAR;
    }

    /**
     * Creates a test input file in the test directory with specified content.
     * Convenience method for subclasses to set up test input.
     *
     * @param fileName the name of the input file
     * @param content the content to write to the file
     * @return the Path to the created file
     * @throws IOException if file creation fails
     */
    protected Path createTestInputFile(String fileName, String content) throws IOException {
        Path inputFile = new Path(testDir, fileName);
        try (var out = remoteFs.create(inputFile)) {
            out.writeBytes(content);
        }
        LOG.debug("Created test input file: {} with {} bytes", inputFile, content.length());
        return inputFile;
    }

    /**
     * Creates a test input directory with a specified number of files.
     * Each file contains the same content.
     *
     * @param dirName the name of the input directory
     * @param numFiles the number of files to create
     * @param content the content for each file
     * @return the Path to the created directory
     * @throws IOException if directory or file creation fails
     */
    protected Path createTestInputDirectory(String dirName, int numFiles, String content)
            throws IOException {
        Path inputDir = new Path(testDir, dirName);
        remoteFs.mkdirs(inputDir);

        for (int i = 0; i < numFiles; i++) {
            Path inputFile = new Path(inputDir, "part-" + i);
            try (var out = remoteFs.create(inputFile)) {
                out.writeBytes(content);
            }
        }
        LOG.debug("Created test input directory: {} with {} files", inputDir, numFiles);
        return inputDir;
    }

    /**
     * Returns the output directory path for the current test.
     *
     * @return the Path to the output directory within testDir
     */
    protected Path getOutputDir() {
        return new Path(testDir, "output");
    }

    /**
     * Returns the input directory path for the current test.
     *
     * @return the Path to the input directory within testDir
     */
    protected Path getInputDir() {
        return new Path(testDir, "input");
    }

    /**
     * Waits for a condition to become true using GenericTestUtils.waitFor().
     * This is a convenience wrapper that uses standard timeout values.
     *
     * @param condition the condition to wait for
     * @param timeoutMs maximum time to wait in milliseconds
     * @throws Exception if the condition is not met within the timeout
     */
    protected void waitForCondition(java.util.function.Supplier<Boolean> condition,
                                    long timeoutMs) throws Exception {
        GenericTestUtils.waitFor(condition, 500, timeoutMs);
    }

    /**
     * Waits for a condition with the default timeout of 60 seconds.
     *
     * @param condition the condition to wait for
     * @throws Exception if the condition is not met within the timeout
     */
    protected void waitForCondition(java.util.function.Supplier<Boolean> condition)
            throws Exception {
        waitForCondition(condition, 60000);
    }
}
