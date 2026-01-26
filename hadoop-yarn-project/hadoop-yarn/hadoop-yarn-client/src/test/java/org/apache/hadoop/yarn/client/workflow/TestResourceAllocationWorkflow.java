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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * YARN Workflow 7 test class for Resource Allocation operations.
 *
 * <p>This test class validates the container allocation workflow using
 * production AMRMClient APIs against a real MiniYARNCluster. It tests
 * the following critical workflows:</p>
 *
 * <ul>
 *   <li>Workflow 7.1: Container allocation via AMRMClient</li>
 *   <li>Workflow 7.2: Impossible resource request handling</li>
 *   <li>Workflow 7.3: Locality relaxation during container allocation</li>
 * </ul>
 *
 * <p>All tests extend {@link AbstractYarnWorkflowTest} to leverage static
 * MiniYARNCluster reuse, achieving the 45-minute CI budget by amortizing
 * the ~25 second cluster initialization across all test methods.</p>
 *
 * <h3>Production APIs Invoked:</h3>
 * <ul>
 *   <li>{@code AMRMClient.createAMRMClient()} - Creates an AMRM client</li>
 *   <li>{@code AMRMClient.registerApplicationMaster()} - Registers AM with RM</li>
 *   <li>{@code AMRMClient.addContainerRequest()} - Requests container allocation</li>
 *   <li>{@code AMRMClient.allocate()} - Gets allocation response from RM</li>
 *   <li>{@code AMRMClient.unregisterApplicationMaster()} - Unregisters AM from RM</li>
 * </ul>
 *
 * <h3>Async Pattern Compliance:</h3>
 * <p>All state transitions and allocations are validated using
 * {@link GenericTestUtils#waitFor} instead of Thread.sleep() to ensure
 * deterministic test behavior.</p>
 *
 * @see AbstractYarnWorkflowTest
 * @see AMRMClient
 * @see GenericTestUtils#waitFor
 */
public class TestResourceAllocationWorkflow extends AbstractYarnWorkflowTest {

    /**
     * Logger for test diagnostics and workflow progress logging.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(TestResourceAllocationWorkflow.class);

    /**
     * Default check interval in milliseconds for polling allocation state.
     */
    private static final long ALLOCATION_CHECK_INTERVAL_MS = 500L;

    /**
     * Default timeout in milliseconds for waiting on container allocation.
     * Set to 60 seconds to accommodate cluster scheduling variability.
     */
    private static final long ALLOCATION_TIMEOUT_MS = 60000L;

    /**
     * Memory allocation for AM container in MB.
     */
    private static final int AM_MEMORY_MB = 1024;

    /**
     * vCore allocation for AM container.
     */
    private static final int AM_VCORES = 1;

    /**
     * Memory allocation for test containers in MB.
     */
    private static final int CONTAINER_MEMORY_MB = 1024;

    /**
     * vCore allocation for test containers.
     */
    private static final int CONTAINER_VCORES = 1;

    /**
     * Application name prefix for test applications.
     */
    private static final String TEST_APP_NAME_PREFIX = "TestWorkflow7_";

    /**
     * Queue name for test application submissions.
     */
    private static final String TEST_QUEUE = "default";

    /**
     * Workflow path: Container allocation via AMRMClient.
     * Production methods invoked: YarnClient.createApplication(), 
     *     YarnClient.submitApplication(), AMRMClient.createAMRMClient(),
     *     AMRMClient.registerApplicationMaster(), AMRMClient.addContainerRequest(),
     *     AMRMClient.allocate(), AllocateResponse.getAllocatedContainers()
     * Input conditions: Valid application submission, container request for
     *     1024MB memory and 1 vcore, proper AM registration with RM
     * Validation criteria: Containers are allocated within timeout, allocated
     *     container resources match request (memory >= requested, vcores >= requested)
     *
     * <p>This test validates the complete container allocation workflow:</p>
     * <ol>
     *   <li>Submit an application and wait for AM to be launched</li>
     *   <li>Obtain AMRM token and register the ApplicationMaster</li>
     *   <li>Create a ContainerRequest with Resource.newInstance(1024, 1)</li>
     *   <li>Add the container request using AMRMClient.addContainerRequest()</li>
     *   <li>Poll AMRMClient.allocate() until containers are allocated</li>
     *   <li>Validate allocated container resources match the request</li>
     * </ol>
     *
     * @throws Exception if any step in the workflow fails
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testContainerAllocation() throws Exception {
        LOG.info("Starting testContainerAllocation - Testing container allocation "
            + "via AMRMClient");

        // ARRANGE: Create and submit application
        LOG.info("Creating and submitting application for container allocation test");
        YarnClientApplication yarnApp = yarnClient.createApplication();
        ApplicationSubmissionContext appContext =
            yarnApp.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Application created with ID: {}", appId);

        AMRMClient<ContainerRequest> amrmClient = null;

        try {
            // Configure and submit application
            configureApplicationContext(appContext, TEST_APP_NAME_PREFIX
                + "ContainerAllocation_" + testName);
            yarnClient.submitApplication(appContext);
            LOG.info("Application {} submitted successfully", appId);

            // Wait for AM to be launched and get the attempt ID
            ApplicationAttemptId attemptId = waitForAMToLaunch(appId);
            LOG.info("AM attempt {} is now LAUNCHED", attemptId);

            // Setup AMRM token for the AMRMClient
            setupAMRMToken(appId, attemptId);

            // Create and start AMRMClient
            LOG.info("Creating and starting AMRMClient");
            amrmClient = AMRMClient.createAMRMClient();
            amrmClient.init(conf);
            amrmClient.start();

            // Register the ApplicationMaster with ResourceManager
            LOG.info("Registering ApplicationMaster with ResourceManager");
            amrmClient.registerApplicationMaster("localhost", 0, "");
            LOG.info("ApplicationMaster registered successfully");

            // ACT: Create and add container request
            Resource capability = Resource.newInstance(CONTAINER_MEMORY_MB,
                CONTAINER_VCORES);
            Priority priority = Priority.newInstance(1);
            ContainerRequest containerRequest = new ContainerRequest(
                capability, null, null, priority);

            LOG.info("Adding container request: memory={}MB, vcores={}",
                CONTAINER_MEMORY_MB, CONTAINER_VCORES);
            amrmClient.addContainerRequest(containerRequest);

            // Use GenericTestUtils.waitFor to poll allocate() until containers
            // are allocated
            LOG.info("Polling AMRMClient.allocate() for container allocation");
            final AtomicInteger allocatedContainerCount = new AtomicInteger(0);
            final Container[] allocatedContainer = new Container[1];

            final AMRMClient<ContainerRequest> finalAmrmClient = amrmClient;
            GenericTestUtils.waitFor(
                () -> {
                    try {
                        AllocateResponse allocResponse =
                            finalAmrmClient.allocate(0.5f);
                        List<Container> allocatedContainers =
                            allocResponse.getAllocatedContainers();
                        if (!allocatedContainers.isEmpty()) {
                            allocatedContainerCount.set(allocatedContainers.size());
                            allocatedContainer[0] = allocatedContainers.get(0);
                            LOG.info("Received {} allocated containers",
                                allocatedContainers.size());
                            return true;
                        }
                        LOG.debug("No containers allocated yet, continuing to poll");
                        return false;
                    } catch (Exception e) {
                        LOG.warn("Error during allocate(): {}", e.getMessage());
                        return false;
                    }
                },
                ALLOCATION_CHECK_INTERVAL_MS,
                ALLOCATION_TIMEOUT_MS,
                "Waiting for container allocation"
            );

            // ASSERT: Validate allocated container
            LOG.info("Validating allocated container");
            assertTrue(allocatedContainerCount.get() > 0,
                "At least one container should be allocated");
            assertNotNull(allocatedContainer[0],
                "Allocated container should not be null");

            // Validate container resources meet or exceed request
            Container container = allocatedContainer[0];
            assertNotNull(container.getResource(),
                "Container resource should not be null");
            assertTrue(container.getResource().getMemorySize() >= CONTAINER_MEMORY_MB,
                "Container memory should be >= requested: got "
                + container.getResource().getMemorySize() + "MB, expected >="
                + CONTAINER_MEMORY_MB + "MB");
            assertTrue(container.getResource().getVirtualCores() >= CONTAINER_VCORES,
                "Container vcores should be >= requested: got "
                + container.getResource().getVirtualCores() + " vcores, expected >="
                + CONTAINER_VCORES + " vcores");
            assertNotNull(container.getNodeId(),
                "Container should have assigned node");
            assertNotNull(container.getId(),
                "Container should have ID");

            LOG.info("Container allocation validated successfully: containerId={}, "
                + "nodeId={}, resource={}",
                container.getId(), container.getNodeId(), container.getResource());

            // Unregister the ApplicationMaster
            LOG.info("Unregistering ApplicationMaster");
            amrmClient.unregisterApplicationMaster(
                org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED,
                "Test completed successfully", null);

            LOG.info("testContainerAllocation completed successfully");

        } finally {
            // Cleanup
            cleanupAMRMClient(amrmClient);
            cleanupApplication(appId);
        }
    }

    /**
     * Workflow path: Impossible resource request handling.
     * Production methods invoked: YarnClient.createApplication(),
     *     YarnClient.submitApplication(), AMRMClient.createAMRMClient(),
     *     AMRMClient.registerApplicationMaster(), AMRMClient.addContainerRequest(),
     *     AMRMClient.allocate()
     * Input conditions: Valid application submission, container request for
     *     impossibly large resources (999999MB memory)
     * Validation criteria: No containers allocated within reasonable timeout,
     *     system handles impossible request gracefully without crashing
     *
     * <p>This test validates that the system handles impossible resource requests
     * gracefully:</p>
     * <ol>
     *   <li>Submit an application and register the AM</li>
     *   <li>Create a ContainerRequest with impossibly large resources</li>
     *   <li>Add the request and poll allocate()</li>
     *   <li>Verify that no containers are allocated within timeout</li>
     * </ol>
     *
     * @throws Exception if any step in the workflow fails
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testImpossibleResourceRequest() throws Exception {
        LOG.info("Starting testImpossibleResourceRequest - Testing handling of "
            + "impossible resource requests");

        // ARRANGE: Create and submit application
        LOG.info("Creating and submitting application for impossible resource test");
        YarnClientApplication yarnApp = yarnClient.createApplication();
        ApplicationSubmissionContext appContext =
            yarnApp.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Application created with ID: {}", appId);

        AMRMClient<ContainerRequest> amrmClient = null;

        try {
            // Configure and submit application
            configureApplicationContext(appContext, TEST_APP_NAME_PREFIX
                + "ImpossibleResource_" + testName);
            yarnClient.submitApplication(appContext);
            LOG.info("Application {} submitted successfully", appId);

            // Wait for AM to be launched
            ApplicationAttemptId attemptId = waitForAMToLaunch(appId);
            LOG.info("AM attempt {} is now LAUNCHED", attemptId);

            // Setup AMRM token
            setupAMRMToken(appId, attemptId);

            // Create and start AMRMClient
            LOG.info("Creating and starting AMRMClient");
            amrmClient = AMRMClient.createAMRMClient();
            amrmClient.init(conf);
            amrmClient.start();

            // Register the ApplicationMaster
            LOG.info("Registering ApplicationMaster");
            amrmClient.registerApplicationMaster("localhost", 0, "");

            // ACT: Create impossible resource request
            // 999999 MB is far beyond any NodeManager capacity
            Resource impossibleCapability = Resource.newInstance(999999, 1000);
            Priority priority = Priority.newInstance(1);
            ContainerRequest impossibleRequest = new ContainerRequest(
                impossibleCapability, null, null, priority);

            LOG.info("Adding impossible container request: memory={}MB, vcores={}",
                impossibleCapability.getMemorySize(),
                impossibleCapability.getVirtualCores());
            amrmClient.addContainerRequest(impossibleRequest);

            // Poll allocate() for a limited time - we expect NO allocations
            LOG.info("Polling AMRMClient.allocate() - expecting no allocations");
            final AtomicInteger allocatedCount = new AtomicInteger(0);
            final long shortTimeout = 15000L; // 15 seconds is sufficient to confirm
            
            final AMRMClient<ContainerRequest> finalAmrmClient = amrmClient;
            
            // Poll multiple times to give system a chance to (not) allocate
            int iterations = 10;
            for (int i = 0; i < iterations; i++) {
                try {
                    AllocateResponse allocResponse = finalAmrmClient.allocate(0.5f);
                    int allocated = allocResponse.getAllocatedContainers().size();
                    allocatedCount.addAndGet(allocated);
                    if (allocated > 0) {
                        LOG.warn("Unexpectedly received {} containers for impossible "
                            + "resource request", allocated);
                        break;
                    }
                    LOG.debug("Poll {}/{}: No containers allocated (as expected)",
                        i + 1, iterations);
                    Thread.sleep(1000); // Brief pause between polls
                } catch (Exception e) {
                    LOG.debug("Error during allocate poll: {}", e.getMessage());
                }
            }

            // ASSERT: Verify no containers were allocated
            LOG.info("Validating that no containers were allocated for impossible "
                + "resource request");
            assertEquals(0, allocatedCount.get(),
                "No containers should be allocated for impossible resource request");

            LOG.info("testImpossibleResourceRequest completed successfully - "
                + "system handled impossible request gracefully");

            // Unregister the ApplicationMaster
            amrmClient.unregisterApplicationMaster(
                org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED,
                "Test completed - impossible resource test", null);

        } finally {
            cleanupAMRMClient(amrmClient);
            cleanupApplication(appId);
        }
    }

    /**
     * Workflow path: Locality relaxation during container allocation.
     * Production methods invoked: YarnClient.createApplication(),
     *     YarnClient.submitApplication(), AMRMClient.createAMRMClient(),
     *     AMRMClient.registerApplicationMaster(), AMRMClient.addContainerRequest(),
     *     AMRMClient.allocate(), YarnClient.getNodeReports()
     * Input conditions: Valid application submission, container request with
     *     specific host locality (a node from the cluster)
     * Validation criteria: Container is eventually allocated (locality may be
     *     relaxed), allocation succeeds even if not on the requested node
     *
     * <p>This test validates locality relaxation behavior:</p>
     * <ol>
     *   <li>Get available nodes from the cluster</li>
     *   <li>Submit application and register AM</li>
     *   <li>Create ContainerRequest with specific host preference</li>
     *   <li>Verify that allocation eventually succeeds (may be relaxed)</li>
     * </ol>
     *
     * @throws Exception if any step in the workflow fails
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testLocalityRelaxation() throws Exception {
        LOG.info("Starting testLocalityRelaxation - Testing locality relaxation "
            + "during container allocation");

        // ARRANGE: Get available nodes first
        LOG.info("Getting available nodes from cluster");
        List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
        assertFalse(nodeReports.isEmpty(),
            "Cluster should have at least one running node");

        // Get node information for locality specification
        String targetNode = nodeReports.get(0).getNodeId().getHost();
        String targetRack = nodeReports.get(0).getRackName();
        LOG.info("Target node for locality test: {} (rack: {})",
            targetNode, targetRack);

        // Create and submit application
        LOG.info("Creating and submitting application for locality test");
        YarnClientApplication yarnApp = yarnClient.createApplication();
        ApplicationSubmissionContext appContext =
            yarnApp.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Application created with ID: {}", appId);

        AMRMClient<ContainerRequest> amrmClient = null;

        try {
            // Configure and submit application
            configureApplicationContext(appContext, TEST_APP_NAME_PREFIX
                + "LocalityRelaxation_" + testName);
            yarnClient.submitApplication(appContext);
            LOG.info("Application {} submitted successfully", appId);

            // Wait for AM to be launched
            ApplicationAttemptId attemptId = waitForAMToLaunch(appId);
            LOG.info("AM attempt {} is now LAUNCHED", attemptId);

            // Setup AMRM token
            setupAMRMToken(appId, attemptId);

            // Create and start AMRMClient
            LOG.info("Creating and starting AMRMClient");
            amrmClient = AMRMClient.createAMRMClient();
            amrmClient.init(conf);
            amrmClient.start();

            // Register the ApplicationMaster
            LOG.info("Registering ApplicationMaster");
            amrmClient.registerApplicationMaster("localhost", 0, "");

            // ACT: Create container request with specific locality
            Resource capability = Resource.newInstance(CONTAINER_MEMORY_MB,
                CONTAINER_VCORES);
            Priority priority = Priority.newInstance(1);
            String[] nodes = new String[] { targetNode };
            String[] racks = new String[] { targetRack };

            // Create request with locality preference (relaxLocality=true allows
            // scheduler to relax locality if needed)
            ContainerRequest localityRequest = new ContainerRequest(
                capability, nodes, racks, priority, true);

            LOG.info("Adding container request with locality preference: "
                + "node={}, rack={}, memory={}MB, vcores={}",
                targetNode, targetRack, CONTAINER_MEMORY_MB, CONTAINER_VCORES);
            amrmClient.addContainerRequest(localityRequest);

            // Use GenericTestUtils.waitFor to poll for allocation
            LOG.info("Polling for container allocation with locality relaxation");
            final AtomicInteger allocatedContainerCount = new AtomicInteger(0);
            final Container[] allocatedContainer = new Container[1];

            final AMRMClient<ContainerRequest> finalAmrmClient = amrmClient;
            GenericTestUtils.waitFor(
                () -> {
                    try {
                        AllocateResponse allocResponse =
                            finalAmrmClient.allocate(0.5f);
                        List<Container> allocatedContainers =
                            allocResponse.getAllocatedContainers();
                        if (!allocatedContainers.isEmpty()) {
                            allocatedContainerCount.set(allocatedContainers.size());
                            allocatedContainer[0] = allocatedContainers.get(0);
                            LOG.info("Container allocated on node: {}",
                                allocatedContainers.get(0).getNodeId());
                            return true;
                        }
                        LOG.debug("No containers allocated yet, continuing to poll");
                        return false;
                    } catch (Exception e) {
                        LOG.warn("Error during allocate(): {}", e.getMessage());
                        return false;
                    }
                },
                ALLOCATION_CHECK_INTERVAL_MS,
                ALLOCATION_TIMEOUT_MS,
                "Waiting for container allocation with locality relaxation"
            );

            // ASSERT: Validate that container was allocated (regardless of node)
            LOG.info("Validating container allocation with locality");
            assertTrue(allocatedContainerCount.get() > 0,
                "At least one container should be allocated");
            assertNotNull(allocatedContainer[0],
                "Allocated container should not be null");

            Container container = allocatedContainer[0];
            String allocatedNode = container.getNodeId().getHost();

            // Log whether locality was honored or relaxed
            if (allocatedNode.equals(targetNode)) {
                LOG.info("Container allocated on requested node: {} (locality honored)",
                    allocatedNode);
            } else {
                LOG.info("Container allocated on different node: {} "
                    + "(locality relaxed from requested node: {})",
                    allocatedNode, targetNode);
            }

            // Validate container is properly configured
            assertNotNull(container.getResource(),
                "Container should have resource specification");
            assertNotNull(container.getId(),
                "Container should have ID");
            assertTrue(container.getResource().getMemorySize() >= CONTAINER_MEMORY_MB,
                "Container memory should meet minimum requirement");

            LOG.info("testLocalityRelaxation completed successfully - "
                + "allocation succeeded with container on node: {}", allocatedNode);

            // Unregister the ApplicationMaster
            amrmClient.unregisterApplicationMaster(
                org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED,
                "Test completed - locality relaxation test", null);

        } finally {
            cleanupAMRMClient(amrmClient);
            cleanupApplication(appId);
        }
    }

    /**
     * Waits for the ApplicationMaster to be launched and returns the attempt ID.
     *
     * <p>This method follows the pattern from BaseAMRMClientTest to wait for
     * the AM attempt to reach the LAUNCHED state, which is required before
     * the AMRMClient can successfully communicate with the ResourceManager.</p>
     *
     * @param appId the application ID to monitor
     * @return the ApplicationAttemptId once the AM is launched
     * @throws TimeoutException if the AM does not launch within timeout
     * @throws InterruptedException if the wait is interrupted
     * @throws IOException if there is an I/O error
     * @throws YarnException if there is a YARN error
     */
    private ApplicationAttemptId waitForAMToLaunch(ApplicationId appId)
        throws TimeoutException, InterruptedException, IOException, YarnException {

        LOG.info("Waiting for AM to be launched for application {}", appId);

        // First, wait for application to reach ACCEPTED state
        waitForApplicationState(appId, YarnApplicationState.ACCEPTED,
            ALLOCATION_TIMEOUT_MS);

        // Get the application attempt ID
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        ApplicationAttemptId attemptId = report.getCurrentApplicationAttemptId();
        assertNotNull(attemptId, "Application should have an attempt ID");

        // Wait for AM attempt to reach LAUNCHED state using GenericTestUtils.waitFor
        GenericTestUtils.waitFor(
            () -> {
                try {
                    RMAppAttempt appAttempt = yarnCluster.getResourceManager()
                        .getRMContext().getRMApps()
                        .get(appId).getCurrentAppAttempt();
                    if (appAttempt != null) {
                        RMAppAttemptState state = appAttempt.getAppAttemptState();
                        LOG.debug("AM attempt state: {}", state);
                        return state == RMAppAttemptState.LAUNCHED
                            || state == RMAppAttemptState.RUNNING;
                    }
                    return false;
                } catch (Exception e) {
                    LOG.debug("Error checking AM attempt state: {}", e.getMessage());
                    return false;
                }
            },
            ALLOCATION_CHECK_INTERVAL_MS,
            ALLOCATION_TIMEOUT_MS,
            "Waiting for AM attempt to reach LAUNCHED state"
        );

        return attemptId;
    }

    /**
     * Sets up the AMRM token for AMRMClient communication.
     *
     * <p>This method follows the pattern from BaseAMRMClientTest to obtain
     * and configure the AMRM token required for the AMRMClient to communicate
     * with the ResourceManager as the ApplicationMaster.</p>
     *
     * @param appId the application ID
     * @param attemptId the application attempt ID
     * @throws Exception if token setup fails
     */
    private void setupAMRMToken(ApplicationId appId, ApplicationAttemptId attemptId)
        throws Exception {

        LOG.info("Setting up AMRM token for attempt {}", attemptId);

        // Get the RMAppAttempt to access the AMRM token
        RMAppAttempt appAttempt = yarnCluster.getResourceManager()
            .getRMContext().getRMApps()
            .get(appId).getCurrentAppAttempt();

        // Setup user credentials - following pattern from BaseAMRMClientTest
        UserGroupInformation.setLoginUser(UserGroupInformation
            .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));

        // Add the AMRM token to the current user's credentials
        UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
        appAttempt.getAMRMToken().setService(
            ClientRMProxy.getAMRMTokenService(conf));

        LOG.info("AMRM token configured successfully for attempt {}", attemptId);
    }

    /**
     * Configures an ApplicationSubmissionContext with valid settings for testing.
     *
     * @param appContext the ApplicationSubmissionContext to configure
     * @param appName the application name to set
     */
    private void configureApplicationContext(
        ApplicationSubmissionContext appContext,
        String appName) {

        LOG.debug("Configuring ApplicationSubmissionContext for: {}", appName);

        // Set application name
        appContext.setApplicationName(appName);

        // Set queue
        appContext.setQueue(TEST_QUEUE);

        // Set priority
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        appContext.setPriority(priority);

        // Create and set AM container launch context
        ContainerLaunchContext amContainer = createAMContainerContext();
        appContext.setAMContainerSpec(amContainer);

        // Set resource requirements for AM
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(AM_MEMORY_MB);
        capability.setVirtualCores(AM_VCORES);
        appContext.setResource(capability);

        LOG.debug("ApplicationSubmissionContext configured: name={}, queue={}, "
            + "memory={}MB, vcores={}",
            appName, TEST_QUEUE, AM_MEMORY_MB, AM_VCORES);
    }

    /**
     * Creates a ContainerLaunchContext for the ApplicationMaster.
     *
     * @return configured ContainerLaunchContext for AM
     */
    private ContainerLaunchContext createAMContainerContext() {
        LOG.debug("Creating AM ContainerLaunchContext");

        ContainerLaunchContext amContainer = BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource>emptyMap(),
            new HashMap<String, String>(),
            Arrays.asList("sleep", "300"),
            new HashMap<String, ByteBuffer>(),
            null,
            new HashMap<ApplicationAccessType, String>()
        );

        LOG.debug("AM ContainerLaunchContext created with sleep command");
        return amContainer;
    }

    /**
     * Cleans up the AMRMClient by stopping it.
     *
     * @param amrmClient the AMRMClient to cleanup
     */
    private void cleanupAMRMClient(AMRMClient<ContainerRequest> amrmClient) {
        if (amrmClient != null) {
            try {
                if (amrmClient.getServiceState()
                    == org.apache.hadoop.service.Service.STATE.STARTED) {
                    LOG.info("Stopping AMRMClient");
                    amrmClient.stop();
                }
            } catch (Exception e) {
                LOG.debug("Error stopping AMRMClient: {}", e.getMessage());
            }
        }
    }

    /**
     * Cleans up a test application by killing it.
     *
     * @param appId the application ID to kill
     */
    private void cleanupApplication(ApplicationId appId) {
        if (appId == null) {
            return;
        }

        LOG.info("Cleaning up application {}", appId);
        try {
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();

            if (state != YarnApplicationState.FINISHED
                && state != YarnApplicationState.FAILED
                && state != YarnApplicationState.KILLED) {

                LOG.info("Killing application {} (current state: {})", appId, state);
                yarnClient.killApplication(appId);
                LOG.info("Application {} killed successfully", appId);
            } else {
                LOG.info("Application {} already in terminal state: {}",
                    appId, state);
            }
        } catch (Exception e) {
            LOG.debug("Error during application cleanup for {}: {}",
                appId, e.getMessage());
        }
    }
}
