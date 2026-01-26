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
package org.apache.hadoop.yarn.client.workflow;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract base class providing static MiniYARNCluster lifecycle management
 * for all YARN workflow tests.
 *
 * <p>This class implements the static cluster reuse pattern to achieve the
 * 45-minute CI budget by amortizing the ~25 second cluster initialization
 * across all test methods. All workflow test classes
 * (TestApplicationSubmissionWorkflow, TestResourceAllocationWorkflow,
 * TestApplicationCompletionWorkflow, TestApplicationStatusWorkflow)
 * should extend this base class.</p>
 *
 * <h3>Cluster Lifecycle:</h3>
 * <ul>
 *   <li>{@code @BeforeAll}: Initializes MiniYARNCluster with 3 NodeManagers,
 *       waits for NodeManagers to connect using GenericTestUtils.waitFor(),
 *       creates and starts YarnClient</li>
 *   <li>{@code @AfterAll}: Stops YarnClient and shuts down MiniYARNCluster</li>
 *   <li>{@code @BeforeEach}: Optional per-test setup hook for subclasses</li>
 *   <li>{@code @AfterEach}: Optional per-test cleanup hook for subclasses</li>
 * </ul>
 *
 * <h3>Protected Fields Available to Subclasses:</h3>
 * <ul>
 *   <li>{@code yarnCluster} - The MiniYARNCluster instance</li>
 *   <li>{@code yarnClient} - An initialized and started YarnClient</li>
 *   <li>{@code conf} - The YarnConfiguration used for cluster initialization</li>
 * </ul>
 *
 * <h3>Configuration Settings:</h3>
 * <p>The cluster is configured with test-optimized settings:</p>
 * <ul>
 *   <li>RM_NM_HEARTBEAT_INTERVAL_MS: 100ms (for fast state propagation)</li>
 *   <li>RM_AM_EXPIRY_INTERVAL_MS: 4000ms (for faster AM timeout testing)</li>
 *   <li>RM_SCHEDULER: CapacityScheduler (standard scheduler)</li>
 *   <li>NM_LOG_RETAIN_SECONDS: 1 (minimal log retention for tests)</li>
 *   <li>RM_SCHEDULER_MINIMUM_ALLOCATION_MB: 512 (allow smaller containers)</li>
 * </ul>
 *
 * @see MiniYARNCluster
 * @see YarnClient
 * @see GenericTestUtils#waitFor
 */
public abstract class AbstractYarnWorkflowTest {

    /**
     * Logger for test diagnostics and cluster lifecycle logging.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(AbstractYarnWorkflowTest.class);

    /**
     * Number of NodeManagers to start in the MiniYARNCluster.
     * Using 3 NodeManagers provides realistic scheduling scenarios
     * while keeping initialization time reasonable.
     */
    private static final int NUM_NODE_MANAGERS = 3;

    /**
     * Number of local directories per NodeManager.
     */
    private static final int NUM_LOCAL_DIRS = 1;

    /**
     * Number of log directories per NodeManager.
     */
    private static final int NUM_LOG_DIRS = 1;

    /**
     * Timeout in milliseconds for waiting for NodeManagers to connect.
     * This is set higher than the default to account for CI variability.
     */
    private static final long NM_CONNECT_TIMEOUT_MS = 60000L;

    /**
     * Check interval in milliseconds for polling NodeManager connection status.
     */
    private static final long NM_CONNECT_CHECK_INTERVAL_MS = 500L;

    /**
     * The MiniYARNCluster instance providing ResourceManager and NodeManagers.
     * This cluster is initialized once in {@link #setUpCluster()} and shared
     * across all test methods in all subclasses.
     */
    protected static MiniYARNCluster yarnCluster;

    /**
     * The YarnClient instance for submitting applications and querying status.
     * This client is initialized and started in {@link #setUpCluster()}.
     */
    protected static YarnClient yarnClient;

    /**
     * The YarnConfiguration used for cluster and client initialization.
     * Contains test-optimized settings for fast execution and deterministic behavior.
     */
    protected static YarnConfiguration conf;

    /**
     * Test name for the current test, available after @BeforeEach.
     * Used for creating unique test directories and diagnostic logging.
     */
    protected String testName;

    /**
     * Initializes the MiniYARNCluster and YarnClient before all tests.
     *
     * <p>This method performs the following operations:</p>
     * <ol>
     *   <li>Creates a YarnConfiguration with test-optimized settings</li>
     *   <li>Initializes MiniYARNCluster with 3 NodeManagers</li>
     *   <li>Starts the cluster and waits for NodeManagers to connect</li>
     *   <li>Creates and starts a YarnClient</li>
     * </ol>
     *
     * <p>The cluster initialization takes approximately 25 seconds, which is
     * amortized across all test methods by using static fields and @BeforeAll.</p>
     *
     * @throws Exception if cluster initialization fails
     */
    @BeforeAll
    public static void setUpCluster() throws Exception {
        LOG.info("Setting up MiniYARNCluster for YARN workflow tests");

        // Create configuration with test-optimized settings
        conf = createTestConfiguration();

        // Initialize MiniYARNCluster with ResourceManager and NodeManagers
        LOG.info("Initializing MiniYARNCluster with {} NodeManagers",
            NUM_NODE_MANAGERS);
        yarnCluster = new MiniYARNCluster(
            AbstractYarnWorkflowTest.class.getSimpleName(),
            NUM_NODE_MANAGERS,
            NUM_LOCAL_DIRS,
            NUM_LOG_DIRS);

        // Initialize and start the cluster
        yarnCluster.init(conf);
        yarnCluster.start();

        // Wait for all NodeManagers to connect to the ResourceManager
        LOG.info("Waiting for {} NodeManagers to connect", NUM_NODE_MANAGERS);
        waitForNodeManagersToConnect();

        // Create and start YarnClient
        LOG.info("Creating and starting YarnClient");
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Verify YarnClient is in STARTED state
        assertTrue(yarnClient.getServiceState() == Service.STATE.STARTED,
            "YarnClient should be in STARTED state");

        LOG.info("MiniYARNCluster setup complete. Cluster is ready for tests.");
    }

    /**
     * Shuts down the YarnClient and MiniYARNCluster after all tests complete.
     *
     * <p>This method ensures proper cleanup by:</p>
     * <ol>
     *   <li>Stopping the YarnClient if it is running</li>
     *   <li>Stopping the MiniYARNCluster if it is running</li>
     * </ol>
     *
     * <p>Both shutdown operations are performed defensively with null checks
     * and state validation to prevent errors during cleanup.</p>
     *
     * @throws Exception if shutdown fails
     */
    @AfterAll
    public static void tearDownCluster() throws Exception {
        LOG.info("Tearing down MiniYARNCluster");

        // Stop YarnClient if started
        if (yarnClient != null) {
            try {
                if (yarnClient.getServiceState() == Service.STATE.STARTED) {
                    LOG.info("Stopping YarnClient");
                    yarnClient.stop();
                }
            } catch (Exception e) {
                LOG.warn("Error stopping YarnClient: {}", e.getMessage(), e);
            }
        }

        // Stop MiniYARNCluster if started
        if (yarnCluster != null) {
            try {
                if (yarnCluster.getServiceState() == Service.STATE.STARTED) {
                    LOG.info("Stopping MiniYARNCluster");
                    yarnCluster.stop();
                }
            } catch (Exception e) {
                LOG.warn("Error stopping MiniYARNCluster: {}", e.getMessage(), e);
            }
        }

        LOG.info("MiniYARNCluster teardown complete");
    }

    /**
     * Per-test setup method that captures the test name for diagnostic logging.
     *
     * <p>Subclasses can override this method to perform additional per-test
     * setup, but must call {@code super.setUp(testInfo)} to preserve the
     * test name capture functionality.</p>
     *
     * @param testInfo JUnit 5 TestInfo providing test metadata
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) {
        testName = testInfo.getDisplayName();
        LOG.info("Starting test: {}", testName);
    }

    /**
     * Per-test cleanup method for resource cleanup after each test.
     *
     * <p>Subclasses can override this method to perform additional per-test
     * cleanup, but should call {@code super.tearDown()} if they need the
     * base class cleanup behavior.</p>
     */
    @AfterEach
    public void tearDown() {
        LOG.info("Completed test: {}", testName);
    }

    /**
     * Creates a YarnConfiguration with test-optimized settings.
     *
     * <p>The configuration includes:</p>
     * <ul>
     *   <li>Short heartbeat interval (100ms) for fast state propagation</li>
     *   <li>Short AM expiry interval (4000ms) for faster timeout testing</li>
     *   <li>CapacityScheduler as the resource scheduler</li>
     *   <li>Minimal log retention (1 second) to reduce disk usage</li>
     *   <li>Lower minimum allocation (512MB) for smaller test containers</li>
     * </ul>
     *
     * @return configured YarnConfiguration
     */
    private static YarnConfiguration createTestConfiguration() {
        YarnConfiguration configuration = new YarnConfiguration();

        // Set the scheduler to CapacityScheduler
        configuration.set(YarnConfiguration.RM_SCHEDULER,
            CapacityScheduler.class.getName());

        // Short heartbeat interval for fast NM state propagation
        configuration.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);

        // Short AM expiry interval for faster timeout testing
        configuration.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 4000);

        // Minimal log retention to reduce disk usage during tests
        configuration.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);

        // Lower minimum allocation to allow smaller test containers
        configuration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            512);

        // Enable opportunistic container allocation for advanced testing scenarios
        configuration.setBoolean(
            YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);

        // Set max queue length for opportunistic containers
        configuration.setInt(
            YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH, 10);

        // Use shorter token rolling interval for security tests
        configuration.setLong(
            YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS, 13);

        LOG.info("Created test configuration with scheduler: {}, " +
            "heartbeat interval: {}ms, AM expiry: {}ms",
            configuration.get(YarnConfiguration.RM_SCHEDULER),
            configuration.getInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, -1),
            configuration.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, -1));

        return configuration;
    }

    /**
     * Waits for all NodeManagers to connect to the ResourceManager.
     *
     * <p>This method uses {@link GenericTestUtils#waitFor} to poll the
     * cluster state until all NodeManagers have registered with the
     * ResourceManager, or until the timeout expires.</p>
     *
     * <p>The implementation follows the Hadoop testing pattern of using
     * GenericTestUtils.waitFor() instead of Thread.sleep() for deterministic
     * async state transitions.</p>
     *
     * @throws TimeoutException if NodeManagers do not connect within timeout
     * @throws InterruptedException if the wait is interrupted
     * @throws YarnException if there is no active ResourceManager
     */
    private static void waitForNodeManagersToConnect()
        throws TimeoutException, InterruptedException, YarnException {
        
        // First, use the built-in waitForNodeManagersToConnect method
        boolean connected = yarnCluster.waitForNodeManagersToConnect(
            NM_CONNECT_TIMEOUT_MS);
        
        if (!connected) {
            // If built-in method fails, use GenericTestUtils.waitFor as backup
            LOG.warn("Built-in waitForNodeManagersToConnect returned false, " +
                "using GenericTestUtils.waitFor() as fallback");
            
            GenericTestUtils.waitFor(
                () -> {
                    try {
                        return yarnCluster.getResourceManager() != null &&
                            yarnCluster.getResourceManager().getClientRMService() != null &&
                            isNodeManagerCountSufficient();
                    } catch (Exception e) {
                        LOG.warn("Error checking NodeManager connection status", e);
                        return false;
                    }
                },
                NM_CONNECT_CHECK_INTERVAL_MS,
                NM_CONNECT_TIMEOUT_MS,
                "Waiting for " + NUM_NODE_MANAGERS + " NodeManagers to connect"
            );
        }

        // Validate that the expected number of NodeManagers are connected
        assertNotNull(yarnCluster.getResourceManager(),
            "ResourceManager should not be null");
        assertTrue(isNodeManagerCountSufficient(),
            "Expected " + NUM_NODE_MANAGERS + " NodeManagers to be connected");

        LOG.info("All {} NodeManagers have connected to the ResourceManager",
            NUM_NODE_MANAGERS);
    }

    /**
     * Checks if the expected number of NodeManagers have registered.
     *
     * @return true if the expected number of NodeManagers are connected
     */
    private static boolean isNodeManagerCountSufficient() {
        try {
            int connectedNMs = yarnCluster.getResourceManager()
                .getClientRMService()
                .getClusterMetrics(
                    org.apache.hadoop.yarn.api.protocolrecords
                        .GetClusterMetricsRequest.newInstance())
                .getClusterMetrics()
                .getNumNodeManagers();
            return connectedNMs >= NUM_NODE_MANAGERS;
        } catch (Exception e) {
            LOG.debug("Error checking NodeManager count", e);
            return false;
        }
    }

    /**
     * Returns the ResourceManager from the MiniYARNCluster.
     *
     * <p>This is a convenience method for subclasses that need direct
     * access to the ResourceManager for advanced test scenarios.</p>
     *
     * @return the ResourceManager instance
     */
    protected org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
        getResourceManager() {
        return yarnCluster.getResourceManager();
    }

    /**
     * Returns the NodeManager at the specified index.
     *
     * <p>This is a convenience method for subclasses that need direct
     * access to specific NodeManagers for advanced test scenarios.</p>
     *
     * @param index the index of the NodeManager (0 to NUM_NODE_MANAGERS-1)
     * @return the NodeManager at the specified index
     */
    protected org.apache.hadoop.yarn.server.nodemanager.NodeManager
        getNodeManager(int index) {
        return yarnCluster.getNodeManager(index);
    }

    /**
     * Waits for an application to reach a specific state.
     *
     * <p>This utility method uses GenericTestUtils.waitFor() to poll the
     * application state until it matches the expected state or times out.
     * This pattern is preferred over Thread.sleep() for deterministic testing.</p>
     *
     * @param appId the application ID to monitor
     * @param expectedState the expected application state
     * @param timeoutMs the timeout in milliseconds
     * @throws TimeoutException if the application does not reach the expected state
     * @throws InterruptedException if the wait is interrupted
     * @throws IOException if there is an I/O error querying the application
     * @throws YarnException if there is a YARN error
     */
    protected void waitForApplicationState(
        org.apache.hadoop.yarn.api.records.ApplicationId appId,
        org.apache.hadoop.yarn.api.records.YarnApplicationState expectedState,
        long timeoutMs)
        throws TimeoutException, InterruptedException, IOException, YarnException {

        LOG.info("Waiting for application {} to reach state {}",
            appId, expectedState);

        GenericTestUtils.waitFor(
            () -> {
                try {
                    org.apache.hadoop.yarn.api.records.ApplicationReport report =
                        yarnClient.getApplicationReport(appId);
                    org.apache.hadoop.yarn.api.records.YarnApplicationState
                        currentState = report.getYarnApplicationState();
                    LOG.debug("Application {} current state: {}", appId, currentState);
                    return currentState == expectedState;
                } catch (Exception e) {
                    LOG.warn("Error getting application report for {}: {}",
                        appId, e.getMessage());
                    return false;
                }
            },
            NM_CONNECT_CHECK_INTERVAL_MS,
            timeoutMs,
            "Waiting for application " + appId + " to reach state " + expectedState
        );

        LOG.info("Application {} has reached state {}", appId, expectedState);
    }

    /**
     * Waits for an application to complete (reach a final state).
     *
     * <p>Final states include: FINISHED, FAILED, KILLED.</p>
     *
     * @param appId the application ID to monitor
     * @param timeoutMs the timeout in milliseconds
     * @return the final application report
     * @throws TimeoutException if the application does not complete within timeout
     * @throws InterruptedException if the wait is interrupted
     * @throws IOException if there is an I/O error querying the application
     * @throws YarnException if there is a YARN error
     */
    protected org.apache.hadoop.yarn.api.records.ApplicationReport
        waitForApplicationCompletion(
            org.apache.hadoop.yarn.api.records.ApplicationId appId,
            long timeoutMs)
        throws TimeoutException, InterruptedException, IOException, YarnException {

        LOG.info("Waiting for application {} to complete", appId);

        GenericTestUtils.waitFor(
            () -> {
                try {
                    org.apache.hadoop.yarn.api.records.ApplicationReport report =
                        yarnClient.getApplicationReport(appId);
                    org.apache.hadoop.yarn.api.records.YarnApplicationState
                        state = report.getYarnApplicationState();
                    return state == org.apache.hadoop.yarn.api.records
                            .YarnApplicationState.FINISHED ||
                        state == org.apache.hadoop.yarn.api.records
                            .YarnApplicationState.FAILED ||
                        state == org.apache.hadoop.yarn.api.records
                            .YarnApplicationState.KILLED;
                } catch (Exception e) {
                    LOG.warn("Error checking application completion for {}: {}",
                        appId, e.getMessage());
                    return false;
                }
            },
            NM_CONNECT_CHECK_INTERVAL_MS,
            timeoutMs,
            "Waiting for application " + appId + " to complete"
        );

        org.apache.hadoop.yarn.api.records.ApplicationReport report =
            yarnClient.getApplicationReport(appId);
        LOG.info("Application {} completed with state {} and final status {}",
            appId, report.getYarnApplicationState(),
            report.getFinalApplicationStatus());

        return report;
    }
}
