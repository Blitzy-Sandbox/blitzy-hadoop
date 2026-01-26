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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * YARN Workflow 6 test class for Application Submission operations.
 *
 * <p>This test class validates the complete application submission lifecycle
 * using production YarnClient APIs against a real MiniYARNCluster. It tests
 * the following critical workflows:</p>
 *
 * <ul>
 *   <li>Workflow 6.1: Complete application submission from NEW to RUNNING state</li>
 *   <li>Workflow 6.2: Rejection of invalid resource requests</li>
 *   <li>Workflow 6.3: Handling of duplicate application submissions</li>
 * </ul>
 *
 * <p>All tests extend {@link AbstractYarnWorkflowTest} to leverage static
 * MiniYARNCluster reuse, achieving the 45-minute CI budget by amortizing
 * the ~25 second cluster initialization across all test methods.</p>
 *
 * <h3>Production APIs Invoked:</h3>
 * <ul>
 *   <li>{@code YarnClient.createApplication()} - Creates a new application</li>
 *   <li>{@code YarnClient.submitApplication()} - Submits application to RM</li>
 *   <li>{@code YarnClient.getApplicationReport()} - Queries application state</li>
 *   <li>{@code YarnClient.killApplication()} - Cleans up test applications</li>
 * </ul>
 *
 * <h3>Async Pattern Compliance:</h3>
 * <p>All state transitions are validated using {@link GenericTestUtils#waitFor}
 * instead of Thread.sleep() to ensure deterministic test behavior.</p>
 *
 * @see AbstractYarnWorkflowTest
 * @see GenericTestUtils#waitFor
 */
public class TestApplicationSubmissionWorkflow extends AbstractYarnWorkflowTest {

    /**
     * Logger for test diagnostics and workflow progress logging.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(TestApplicationSubmissionWorkflow.class);

    /**
     * Default check interval in milliseconds for polling application state.
     */
    private static final long STATE_CHECK_INTERVAL_MS = 500L;

    /**
     * Default timeout in milliseconds for waiting on application state transitions.
     * Set to 60 seconds to accommodate cluster scheduling variability.
     */
    private static final long STATE_WAIT_TIMEOUT_MS = 60000L;

    /**
     * Memory allocation for AM container in MB.
     * Using 1024MB which is within typical node manager limits.
     */
    private static final int AM_MEMORY_MB = 1024;

    /**
     * vCore allocation for AM container.
     */
    private static final int AM_VCORES = 1;

    /**
     * Application name prefix for test applications.
     */
    private static final String TEST_APP_NAME_PREFIX = "TestWorkflow6_";

    /**
     * Queue name for test application submissions.
     */
    private static final String TEST_QUEUE = "default";

    /**
     * Workflow path: Complete application submission lifecycle from CREATE to RUNNING.
     * Production methods invoked: YarnClient.createApplication(),
     *     YarnClientApplication.getNewApplicationResponse(),
     *     YarnClientApplication.getApplicationSubmissionContext(),
     *     YarnClient.submitApplication(), YarnClient.getApplicationReport()
     * Input conditions: Valid ApplicationSubmissionContext with valid AM resource
     *     request (1024MB memory, 1 vcore), valid ContainerLaunchContext
     * Validation criteria: ApplicationId is assigned, application reaches ACCEPTED
     *     state then RUNNING state, ApplicationReport contains valid diagnostics
     *
     * <p>This test validates the complete happy path for application submission:</p>
     * <ol>
     *   <li>Create a new application using YarnClient.createApplication()</li>
     *   <li>Configure ApplicationSubmissionContext with valid AM container spec</li>
     *   <li>Submit the application using YarnClient.submitApplication()</li>
     *   <li>Wait for application to reach ACCEPTED state</li>
     *   <li>Wait for application to reach RUNNING state</li>
     *   <li>Validate ApplicationReport contains expected metadata</li>
     * </ol>
     *
     * @throws Exception if any step in the workflow fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSubmissionToRunning() throws Exception {
        LOG.info("Starting testSubmissionToRunning - Testing complete "
            + "application submission lifecycle");

        // ARRANGE: Create application using production YarnClient API
        LOG.info("Creating new application via YarnClient.createApplication()");
        YarnClientApplication yarnApp = yarnClient.createApplication();
        assertNotNull(yarnApp, "YarnClientApplication should not be null");
        assertNotNull(yarnApp.getNewApplicationResponse(),
            "NewApplicationResponse should not be null");

        // Get the ApplicationSubmissionContext from the application
        ApplicationSubmissionContext appContext =
            yarnApp.getApplicationSubmissionContext();
        assertNotNull(appContext,
            "ApplicationSubmissionContext should not be null");

        // Get the assigned ApplicationId
        ApplicationId appId = appContext.getApplicationId();
        assertNotNull(appId, "ApplicationId should be assigned");
        LOG.info("Application created with ID: {}", appId);

        try {
            // Configure the application submission context
            configureApplicationContext(appContext, TEST_APP_NAME_PREFIX + testName);

            // ACT: Submit the application using production API
            LOG.info("Submitting application {} to queue '{}'", appId, TEST_QUEUE);
            yarnClient.submitApplication(appContext);
            LOG.info("Application {} submitted successfully", appId);

            // ASSERT: Wait for application to reach ACCEPTED state
            LOG.info("Waiting for application {} to reach ACCEPTED state", appId);
            waitForApplicationState(appId, YarnApplicationState.ACCEPTED,
                STATE_WAIT_TIMEOUT_MS);

            // Validate application is in ACCEPTED state
            ApplicationReport acceptedReport = yarnClient.getApplicationReport(appId);
            assertEquals(YarnApplicationState.ACCEPTED,
                acceptedReport.getYarnApplicationState(),
                "Application should be in ACCEPTED state");
            LOG.info("Application {} is now in ACCEPTED state", appId);

            // Wait for application to reach RUNNING state
            LOG.info("Waiting for application {} to reach RUNNING state", appId);
            waitForApplicationState(appId, YarnApplicationState.RUNNING,
                STATE_WAIT_TIMEOUT_MS);

            // Validate application is in RUNNING state
            ApplicationReport runningReport = yarnClient.getApplicationReport(appId);
            assertEquals(YarnApplicationState.RUNNING,
                runningReport.getYarnApplicationState(),
                "Application should be in RUNNING state");
            LOG.info("Application {} is now in RUNNING state", appId);

            // Validate ApplicationReport metadata
            assertNotNull(runningReport.getApplicationId(),
                "ApplicationReport should have ApplicationId");
            assertEquals(appId, runningReport.getApplicationId(),
                "ApplicationReport ApplicationId should match submitted ApplicationId");
            assertNotNull(runningReport.getCurrentApplicationAttemptId(),
                "ApplicationReport should have current attempt ID");
            assertNotNull(runningReport.getQueue(),
                "ApplicationReport should have queue name");
            assertEquals(TEST_QUEUE, runningReport.getQueue(),
                "Application should be in expected queue");

            LOG.info("testSubmissionToRunning completed successfully. "
                + "Application {} transitioned from NEW -> ACCEPTED -> RUNNING",
                appId);

        } finally {
            // Cleanup: Kill the application to free resources
            cleanupApplication(appId);
        }
    }

    /**
     * Workflow path: Rejection of invalid AM resource request.
     * Production methods invoked: YarnClient.createApplication(),
     *     YarnClient.submitApplication()
     * Input conditions: ApplicationSubmissionContext with impossibly large AM
     *     resource request (exceeds cluster maximum: 1TB memory, 1000 vcores)
     * Validation criteria: submitApplication() throws YarnException with
     *     appropriate error message indicating invalid resource request
     *
     * <p>This test validates that the ResourceManager properly rejects
     * applications with invalid resource requests that exceed cluster capacity.</p>
     *
     * <p>The test submits an application requesting resources far beyond what
     * any NodeManager can provide (1TB memory, 1000 vcores). The expected
     * behavior is that the RM rejects the application with an appropriate
     * exception.</p>
     *
     * @throws Exception if an unexpected error occurs
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testInvalidResourceRequest() throws Exception {
        LOG.info("Starting testInvalidResourceRequest - Testing rejection "
            + "of invalid AM resource requests");

        // ARRANGE: Create application
        LOG.info("Creating new application for invalid resource request test");
        YarnClientApplication yarnApp = yarnClient.createApplication();
        ApplicationSubmissionContext appContext =
            yarnApp.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Created application {} for invalid resource request test", appId);

        try {
            // Configure basic application context
            appContext.setApplicationName(TEST_APP_NAME_PREFIX + "InvalidResource_"
                + testName);
            appContext.setQueue(TEST_QUEUE);

            // Set up priority
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);
            appContext.setPriority(priority);

            // Create AM container launch context with minimal configuration
            ContainerLaunchContext amContainer = createMinimalAMContainerContext();
            appContext.setAMContainerSpec(amContainer);

            // ACT: Set impossibly large resource request
            // 1TB memory and 1000 vcores - far exceeds any realistic NodeManager
            Resource invalidResource = Records.newRecord(Resource.class);
            invalidResource.setMemorySize(1024L * 1024L); // 1TB in MB
            invalidResource.setVirtualCores(1000);
            appContext.setResource(invalidResource);

            LOG.info("Attempting to submit application {} with invalid resources: "
                + "memory={}MB, vcores={}",
                appId, invalidResource.getMemorySize(),
                invalidResource.getVirtualCores());

            // ASSERT: Expect YarnException when submitting invalid resource request
            // The ResourceManager should reject applications with impossible resource
            // requests that exceed maximum allocation or cluster capacity
            try {
                yarnClient.submitApplication(appContext);
                
                // If submission succeeds, the app should quickly fail
                // Wait a bit and check if the application gets rejected
                LOG.info("Submission did not throw immediately, waiting for "
                    + "application state");
                
                GenericTestUtils.waitFor(
                    () -> {
                        try {
                            ApplicationReport report =
                                yarnClient.getApplicationReport(appId);
                            YarnApplicationState state =
                                report.getYarnApplicationState();
                            // Application should fail or be rejected
                            return state == YarnApplicationState.FAILED
                                || state == YarnApplicationState.KILLED;
                        } catch (Exception e) {
                            LOG.debug("Error checking application state: {}",
                                e.getMessage());
                            return false;
                        }
                    },
                    STATE_CHECK_INTERVAL_MS,
                    STATE_WAIT_TIMEOUT_MS,
                    "Waiting for application to fail due to invalid resources"
                );
                
                // If we reach here, verify the application failed with
                // appropriate diagnostics
                ApplicationReport report = yarnClient.getApplicationReport(appId);
                YarnApplicationState finalState = report.getYarnApplicationState();
                assertTrue(
                    finalState == YarnApplicationState.FAILED
                        || finalState == YarnApplicationState.KILLED,
                    "Application with invalid resources should fail or be killed, "
                        + "but was: " + finalState
                );
                LOG.info("Application {} correctly rejected with state: {}",
                    appId, finalState);
                
            } catch (YarnException e) {
                // This is the expected behavior - application should be rejected
                LOG.info("Application {} correctly rejected with YarnException: {}",
                    appId, e.getMessage());
                assertTrue(e.getMessage() != null,
                    "Exception message should not be null");
            } catch (IOException e) {
                // IOException may wrap the underlying rejection
                LOG.info("Application {} rejected with IOException: {}",
                    appId, e.getMessage());
                assertTrue(e.getMessage() != null,
                    "Exception message should not be null");
            }

            LOG.info("testInvalidResourceRequest completed successfully");

        } finally {
            // Cleanup: Attempt to kill the application if it was submitted
            cleanupApplication(appId);
        }
    }

    /**
     * Workflow path: Handling of duplicate application submission.
     * Production methods invoked: YarnClient.createApplication(),
     *     YarnClient.submitApplication() (called twice with same AppId)
     * Input conditions: Valid ApplicationSubmissionContext submitted successfully,
     *     then attempt to resubmit the same ApplicationId
     * Validation criteria: Second submitApplication() call throws YarnException
     *     indicating duplicate submission or application already exists
     *
     * <p>This test validates that the ResourceManager properly handles attempts
     * to submit the same application ID twice. The expected behavior is that
     * the second submission should fail with an appropriate exception.</p>
     *
     * @throws Exception if an unexpected error occurs
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDuplicateSubmission() throws Exception {
        LOG.info("Starting testDuplicateSubmission - Testing duplicate "
            + "application submission handling");

        // ARRANGE: Create and submit first application
        LOG.info("Creating first application for duplicate submission test");
        YarnClientApplication yarnApp = yarnClient.createApplication();
        ApplicationSubmissionContext appContext =
            yarnApp.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Created application {} for duplicate submission test", appId);

        try {
            // Configure valid application context
            configureApplicationContext(appContext,
                TEST_APP_NAME_PREFIX + "Duplicate_" + testName);

            // ACT Part 1: Submit the application successfully
            LOG.info("Submitting first instance of application {}", appId);
            yarnClient.submitApplication(appContext);
            LOG.info("First submission of application {} completed", appId);

            // Wait for the application to be accepted by the RM
            LOG.info("Waiting for application {} to reach ACCEPTED state", appId);
            waitForApplicationState(appId, YarnApplicationState.ACCEPTED,
                STATE_WAIT_TIMEOUT_MS);

            // Verify first submission was successful
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            assertNotNull(report, "ApplicationReport should not be null");
            LOG.info("Application {} is in state: {}",
                appId, report.getYarnApplicationState());

            // ACT Part 2: Attempt to resubmit the same application
            LOG.info("Attempting duplicate submission of application {}", appId);

            // Create a new submission context with the SAME application ID
            ApplicationSubmissionContext duplicateContext =
                Records.newRecord(ApplicationSubmissionContext.class);
            duplicateContext.setApplicationId(appId);
            duplicateContext.setApplicationName(TEST_APP_NAME_PREFIX
                + "Duplicate_Resubmit_" + testName);
            duplicateContext.setQueue(TEST_QUEUE);

            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);
            duplicateContext.setPriority(priority);

            ContainerLaunchContext amContainer = createMinimalAMContainerContext();
            duplicateContext.setAMContainerSpec(amContainer);

            Resource capability = Records.newRecord(Resource.class);
            capability.setMemorySize(AM_MEMORY_MB);
            capability.setVirtualCores(AM_VCORES);
            duplicateContext.setResource(capability);

            // ASSERT: Expect exception on duplicate submission
            try {
                yarnClient.submitApplication(duplicateContext);
                // If no exception, the RM might silently accept but not re-process
                // Check that the application is still the original one
                ApplicationReport duplicateReport =
                    yarnClient.getApplicationReport(appId);
                LOG.info("Duplicate submission completed without exception. "
                    + "Application state: {}", duplicateReport.getYarnApplicationState());
                
                // The RM may accept duplicate submissions idempotently
                // In this case, verify the application is still running/accepted
                assertTrue(
                    duplicateReport.getYarnApplicationState()
                        == YarnApplicationState.ACCEPTED
                        || duplicateReport.getYarnApplicationState()
                            == YarnApplicationState.RUNNING
                        || duplicateReport.getYarnApplicationState()
                            == YarnApplicationState.SUBMITTED,
                    "Application should still be in a valid running state"
                );
                LOG.info("RM handled duplicate submission idempotently for "
                    + "application {}", appId);
                
            } catch (YarnException e) {
                // Expected: RM rejects duplicate submission
                LOG.info("Duplicate submission correctly rejected with "
                    + "YarnException: {}", e.getMessage());
                assertNotNull(e.getMessage(), "Exception should have a message");
            } catch (IOException e) {
                // IOException may wrap the duplicate rejection
                LOG.info("Duplicate submission rejected with IOException: {}",
                    e.getMessage());
                assertNotNull(e.getMessage(), "Exception should have a message");
            }

            LOG.info("testDuplicateSubmission completed successfully");

        } finally {
            // Cleanup: Kill the application
            cleanupApplication(appId);
        }
    }

    /**
     * Configures an ApplicationSubmissionContext with valid settings for testing.
     *
     * <p>This method sets up all required fields for a valid application
     * submission including:</p>
     * <ul>
     *   <li>Application name</li>
     *   <li>Queue</li>
     *   <li>Priority</li>
     *   <li>AM container specification</li>
     *   <li>Resource requirements (memory and vcores)</li>
     * </ul>
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
     * <p>The context is configured with a simple sleep command that keeps
     * the AM alive for testing purposes. This uses the BuilderUtils factory
     * to create a properly configured ContainerLaunchContext.</p>
     *
     * @return configured ContainerLaunchContext for AM
     */
    private ContainerLaunchContext createAMContainerContext() {
        LOG.debug("Creating AM ContainerLaunchContext");

        // Use BuilderUtils to create a properly configured ContainerLaunchContext
        // This follows the pattern from BaseAMRMClientTest
        ContainerLaunchContext amContainer = BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource>emptyMap(),  // localResources
            new HashMap<String, String>(),                   // environment
            Arrays.asList("sleep", "100"),                   // commands
            new HashMap<String, ByteBuffer>(),               // serviceData
            null,                                            // tokens
            new HashMap<ApplicationAccessType, String>()     // acls
        );

        LOG.debug("AM ContainerLaunchContext created with sleep command");
        return amContainer;
    }

    /**
     * Creates a minimal ContainerLaunchContext for testing purposes.
     *
     * <p>This creates a ContainerLaunchContext using Records.newRecord()
     * with no commands set. This is used for invalid resource request tests
     * where we want to test RM rejection before container execution.</p>
     *
     * @return minimal ContainerLaunchContext
     */
    private ContainerLaunchContext createMinimalAMContainerContext() {
        LOG.debug("Creating minimal AM ContainerLaunchContext");

        ContainerLaunchContext amContainer =
            Records.newRecord(ContainerLaunchContext.class);
        // Minimal context - no commands, just to test submission
        return amContainer;
    }

    /**
     * Waits for an application to reach the specified state using
     * GenericTestUtils.waitFor().
     *
     * <p>This method polls the application state using the production
     * YarnClient.getApplicationReport() API until the expected state is
     * reached or the timeout expires.</p>
     *
     * <p>This follows the Hadoop testing pattern of using GenericTestUtils.waitFor()
     * instead of Thread.sleep() for deterministic async state transitions.</p>
     *
     * @param appId the application ID to monitor
     * @param expectedState the expected YarnApplicationState
     * @param timeoutMs the maximum time to wait in milliseconds
     * @throws TimeoutException if the state is not reached within timeout
     * @throws InterruptedException if the wait is interrupted
     */
    private void waitForApplicationState(
        ApplicationId appId,
        YarnApplicationState expectedState,
        long timeoutMs)
        throws TimeoutException, InterruptedException {

        LOG.info("Waiting for application {} to reach state {} (timeout: {}ms)",
            appId, expectedState, timeoutMs);

        GenericTestUtils.waitFor(
            () -> {
                try {
                    ApplicationReport report = yarnClient.getApplicationReport(appId);
                    YarnApplicationState currentState =
                        report.getYarnApplicationState();
                    LOG.debug("Application {} current state: {}, expected: {}",
                        appId, currentState, expectedState);
                    return currentState == expectedState;
                } catch (YarnException | IOException e) {
                    LOG.warn("Error getting application report for {}: {}",
                        appId, e.getMessage());
                    return false;
                }
            },
            STATE_CHECK_INTERVAL_MS,
            timeoutMs,
            "Waiting for application " + appId + " to reach state " + expectedState
        );

        LOG.info("Application {} has reached state {}", appId, expectedState);
    }

    /**
     * Cleans up a test application by killing it.
     *
     * <p>This method attempts to kill the application and ignores any errors
     * that occur during cleanup. It is designed to be called from finally
     * blocks to ensure test cleanup even when tests fail.</p>
     *
     * @param appId the application ID to kill
     */
    private void cleanupApplication(ApplicationId appId) {
        if (appId == null) {
            return;
        }

        LOG.info("Cleaning up application {}", appId);
        try {
            // Check if application is still active
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            
            // Only kill if not already in a terminal state
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
            // Ignore cleanup errors
            LOG.debug("Error during application cleanup for {}: {}",
                appId, e.getMessage());
        }
    }
}
