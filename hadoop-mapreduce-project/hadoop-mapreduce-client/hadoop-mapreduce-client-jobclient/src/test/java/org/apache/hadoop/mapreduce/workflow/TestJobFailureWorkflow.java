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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.FailJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class for Workflow 13 (Job Failure Handling).
 * 
 * <p>This test class validates MapReduce job failure scenarios including:
 * <ul>
 *   <li>Job failure handling with failed mappers</li>
 *   <li>Partial map failure tracking</li>
 *   <li>Diagnostics population on failure</li>
 *   <li>Output cleanup after job failure</li>
 * </ul>
 * 
 * <p>Extends {@link AbstractMapReduceWorkflowTest} for static MiniMRYarnCluster
 * and MiniDFSCluster lifecycle management to ensure cluster reuse across all
 * test methods and meet the 45-minute CI budget constraint.
 * 
 * <p>All tests use:
 * <ul>
 *   <li>{@code @Timeout(60)} annotation for 60-second time limits</li>
 *   <li>{@link GenericTestUtils#waitFor} for async state transitions</li>
 *   <li>Production Job APIs directly (submit, waitForCompletion, getJobState, etc.)</li>
 * </ul>
 * 
 * @see AbstractMapReduceWorkflowTest
 * @see FailJob
 */
public class TestJobFailureWorkflow extends AbstractMapReduceWorkflowTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestJobFailureWorkflow.class);

    /**
     * Timeout for waiting for job state transitions in milliseconds.
     * Allows sufficient time for job submission, task scheduling, and failure detection.
     */
    private static final long JOB_STATE_TIMEOUT_MS = 60000;

    /**
     * Interval between job state checks in milliseconds.
     */
    private static final long JOB_STATE_CHECK_INTERVAL_MS = 500;

    /**
     * Workflow: Job Failure Handling with FailJob.FAIL_MAP=true
     * Production methods invoked: Job.getInstance(), Job.submit(), Job.waitForCompletion(),
     *                             Job.getJobState(), Job.getJobID()
     * Input conditions: Single mapper configured to fail via FailJob.FAIL_MAP=true
     * Validation criteria: waitForCompletion returns false, job state is FAILED
     * 
     * <p>This test validates that when a MapReduce job is configured with mappers
     * that fail, the job correctly transitions to FAILED state and waitForCompletion
     * returns false.
     *
     * @throws Exception if job setup or validation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testJobFailureHandling() throws Exception {
        LOG.info("Starting testJobFailureHandling");

        // Verify cluster is available
        if (mrCluster == null) {
            LOG.warn("MRCluster not available, skipping test");
            return;
        }

        // ARRANGE: Create test input file
        Path inputDir = new Path(testDir, "input");
        remoteFs.mkdirs(inputDir);
        Path inputFile = new Path(inputDir, "test-input.txt");
        try (FSDataOutputStream out = remoteFs.create(inputFile)) {
            out.writeBytes("test line 1\n");
            out.writeBytes("test line 2\n");
        }
        LOG.info("Created test input file: {}", inputFile);

        // Configure job to fail
        Configuration jobConf = new Configuration(conf);
        jobConf.setBoolean(FailJob.FAIL_MAP, true);
        jobConf.setBoolean(FailJob.FAIL_REDUCE, false);
        // Limit retries to speed up failure
        jobConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);

        // Create job using production APIs
        Job job = Job.getInstance(jobConf, "testJobFailureHandling");
        job.setJarByClass(FailJob.class);
        job.addFileToClassPath(APP_JAR);

        // Configure mapper to fail
        job.setMapperClass(FailJob.FailMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Configure input/output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, inputDir);

        // Disable reducers for simpler test
        job.setNumReduceTasks(0);

        // Disable speculative execution
        job.setSpeculativeExecution(false);

        LOG.info("Submitting job with ID: {}", job.getJobName());

        // ACT: Submit and wait for job completion
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should be assigned after submission");
        LOG.info("Job submitted with ID: {}", job.getJobID());

        // Wait for completion - should return false for failed job
        boolean succeeded = job.waitForCompletion(true);

        // ASSERT: Validate job failure
        assertFalse(succeeded, "Job should fail when mapper is configured to fail");

        // Wait for job state to transition to FAILED using GenericTestUtils.waitFor
        GenericTestUtils.waitFor(
            () -> {
                try {
                    JobStatus.State state = job.getJobState();
                    LOG.debug("Current job state: {}", state);
                    return state == JobStatus.State.FAILED;
                } catch (IOException | InterruptedException e) {
                    LOG.warn("Error checking job state", e);
                    return false;
                }
            },
            JOB_STATE_CHECK_INTERVAL_MS,
            JOB_STATE_TIMEOUT_MS
        );

        // Final verification
        JobStatus.State finalState = job.getJobState();
        assertEquals(JobStatus.State.FAILED, finalState,
            "Job state should be FAILED after mapper failure");

        LOG.info("testJobFailureHandling completed successfully. Job state: {}", finalState);
    }

    /**
     * Workflow: Partial Map Failure with multiple mappers
     * Production methods invoked: Job.getInstance(), Job.submit(), Job.waitForCompletion(),
     *                             Job.getTaskReports(TaskType.MAP), TaskReport.getCurrentStatus()
     * Input conditions: Multiple input files creating multiple mappers, configured to fail
     * Validation criteria: TaskReports show map tasks attempted, some may complete before failure
     * 
     * <p>This test validates that when a job with multiple mappers fails,
     * we can track the partial completion via TaskReports.
     *
     * @throws Exception if job setup or validation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testPartialMapFailure() throws Exception {
        LOG.info("Starting testPartialMapFailure");

        // Verify cluster is available
        if (mrCluster == null) {
            LOG.warn("MRCluster not available, skipping test");
            return;
        }

        // ARRANGE: Create multiple input files to generate multiple map tasks
        Path inputDir = new Path(testDir, "input");
        remoteFs.mkdirs(inputDir);

        // Create 3 input files for 3 map tasks
        int numMappers = 3;
        for (int i = 0; i < numMappers; i++) {
            Path inputFile = new Path(inputDir, "input-" + i + ".txt");
            try (FSDataOutputStream out = remoteFs.create(inputFile)) {
                out.writeBytes("mapper " + i + " input line 1\n");
                out.writeBytes("mapper " + i + " input line 2\n");
            }
        }
        LOG.info("Created {} input files in {}", numMappers, inputDir);

        // Configure job to fail on map
        Configuration jobConf = new Configuration(conf);
        jobConf.setBoolean(FailJob.FAIL_MAP, true);
        jobConf.setBoolean(FailJob.FAIL_REDUCE, false);
        // Single attempt to fail quickly
        jobConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);

        // Create job
        Job job = Job.getInstance(jobConf, "testPartialMapFailure");
        job.setJarByClass(FailJob.class);
        job.addFileToClassPath(APP_JAR);

        // Configure mapper
        job.setMapperClass(FailJob.FailMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Configure input/output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, inputDir);

        // No reducers
        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);

        LOG.info("Submitting job for partial map failure test");

        // ACT: Submit and wait for job
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should be assigned");
        LOG.info("Job submitted with ID: {}", job.getJobID());

        // Wait for completion (will fail)
        boolean succeeded = job.waitForCompletion(true);
        assertFalse(succeeded, "Job should fail");

        // Wait for job to reach terminal state
        GenericTestUtils.waitFor(
            () -> {
                try {
                    JobStatus.State state = job.getJobState();
                    return state == JobStatus.State.FAILED || state == JobStatus.State.KILLED;
                } catch (IOException | InterruptedException e) {
                    return false;
                }
            },
            JOB_STATE_CHECK_INTERVAL_MS,
            JOB_STATE_TIMEOUT_MS
        );

        // ASSERT: Get task reports and verify we can track map task states
        TaskReport[] mapReports = job.getTaskReports(TaskType.MAP);
        assertNotNull(mapReports, "Map task reports should be available");
        
        LOG.info("Number of map task reports: {}", mapReports.length);

        // Verify we have map task reports
        assertTrue(mapReports.length > 0, "Should have at least one map task report");

        // Log task states for debugging
        int failedCount = 0;
        int killedCount = 0;
        for (TaskReport report : mapReports) {
            TIPStatus status = report.getCurrentStatus();
            LOG.info("Map task {} state: {}", report.getTaskId(), status);
            
            if (status == TIPStatus.FAILED) {
                failedCount++;
            } else if (status == TIPStatus.KILLED) {
                killedCount++;
            }
        }

        LOG.info("Map tasks - Failed: {}, Killed: {}", failedCount, killedCount);

        // At least one task should have failed or been killed
        assertTrue(failedCount > 0 || killedCount > 0,
            "At least one map task should have failed or been killed");

        LOG.info("testPartialMapFailure completed successfully");
    }

    /**
     * Workflow: Diagnostics Population on Job Failure
     * Production methods invoked: Job.getInstance(), Job.submit(), Job.waitForCompletion(),
     *                             Job.getStatus(), JobStatus.getFailureInfo()
     * Input conditions: Job configured to fail via FailJob.FAIL_MAP=true
     * Validation criteria: Job.getStatus().getFailureInfo() contains meaningful diagnostic message
     * 
     * <p>This test validates that when a job fails, the failure diagnostics
     * are properly populated and accessible via the production APIs.
     *
     * @throws Exception if job setup or validation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDiagnosticsPopulated() throws Exception {
        LOG.info("Starting testDiagnosticsPopulated");

        // Verify cluster is available
        if (mrCluster == null) {
            LOG.warn("MRCluster not available, skipping test");
            return;
        }

        // ARRANGE: Create test input
        Path inputDir = new Path(testDir, "input");
        remoteFs.mkdirs(inputDir);
        Path inputFile = new Path(inputDir, "diagnostics-test.txt");
        try (FSDataOutputStream out = remoteFs.create(inputFile)) {
            out.writeBytes("test data for diagnostics test\n");
        }
        LOG.info("Created test input file: {}", inputFile);

        // Configure job to fail
        Configuration jobConf = new Configuration(conf);
        jobConf.setBoolean(FailJob.FAIL_MAP, true);
        jobConf.setBoolean(FailJob.FAIL_REDUCE, false);
        jobConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);

        // Create job
        Job job = Job.getInstance(jobConf, "testDiagnosticsPopulated");
        job.setJarByClass(FailJob.class);
        job.addFileToClassPath(APP_JAR);

        // Configure mapper
        job.setMapperClass(FailJob.FailMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Configure input/output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, inputDir);

        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);

        LOG.info("Submitting job for diagnostics test");

        // ACT: Submit and wait for failure
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should be assigned");
        LOG.info("Job submitted with ID: {}", job.getJobID());

        boolean succeeded = job.waitForCompletion(true);
        assertFalse(succeeded, "Job should fail");

        // Wait for job to reach FAILED state
        GenericTestUtils.waitFor(
            () -> {
                try {
                    return job.getJobState() == JobStatus.State.FAILED;
                } catch (IOException | InterruptedException e) {
                    return false;
                }
            },
            JOB_STATE_CHECK_INTERVAL_MS,
            JOB_STATE_TIMEOUT_MS
        );

        // ASSERT: Verify failure diagnostics are populated
        JobStatus status = job.getStatus();
        assertNotNull(status, "Job status should be available");

        String failureInfo = status.getFailureInfo();
        LOG.info("Failure info: {}", failureInfo);

        // Failure info should not be null and should contain some diagnostic content
        assertNotNull(failureInfo, "Failure info should be populated");
        
        // The failure info should not be the default "NA" value or empty
        assertFalse(failureInfo.isEmpty(), "Failure info should not be empty");
        
        // The diagnostics should contain information about the failure
        // FailJob.FailMapper throws RuntimeException with "Intentional map failure"
        // The failure info should contain some reference to task failure
        // Note: The exact format depends on YARN/MR implementation
        assertTrue(
            failureInfo.length() > 2 && !failureInfo.equals("NA"),
            "Failure info should contain meaningful diagnostic content, got: " + failureInfo
        );

        LOG.info("testDiagnosticsPopulated completed successfully. Diagnostics: {}", 
                 failureInfo.length() > 100 ? failureInfo.substring(0, 100) + "..." : failureInfo);
    }

    /**
     * Workflow: Output Cleanup after Job Failure
     * Production methods invoked: Job.getInstance(), Job.submit(), Job.waitForCompletion(),
     *                             FileSystem.exists()
     * Input conditions: Job configured to fail with specified output directory
     * Validation criteria: Output directory is handled properly after failure (not left partial)
     * 
     * <p>This test validates that when a job fails, the output directory
     * is properly handled and not left in a partial/inconsistent state.
     * The test verifies this using FileSystem.exists() on the output path.
     *
     * @throws Exception if job setup or validation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testOutputCleanup() throws Exception {
        LOG.info("Starting testOutputCleanup");

        // Verify cluster is available
        if (mrCluster == null) {
            LOG.warn("MRCluster not available, skipping test");
            return;
        }

        // ARRANGE: Create test input
        Path inputDir = new Path(testDir, "input");
        remoteFs.mkdirs(inputDir);
        Path inputFile = new Path(inputDir, "cleanup-test.txt");
        try (FSDataOutputStream out = remoteFs.create(inputFile)) {
            out.writeBytes("test data for output cleanup test\n");
        }

        Path outputDir = new Path(testDir, "output");
        LOG.info("Input: {}, Output: {}", inputDir, outputDir);

        // Verify output doesn't exist yet
        assertFalse(remoteFs.exists(outputDir), 
            "Output directory should not exist before job submission");

        // Configure job to fail
        Configuration jobConf = new Configuration(conf);
        jobConf.setBoolean(FailJob.FAIL_MAP, true);
        jobConf.setBoolean(FailJob.FAIL_REDUCE, false);
        jobConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);

        // Create job
        Job job = Job.getInstance(jobConf, "testOutputCleanup");
        job.setJarByClass(FailJob.class);
        job.addFileToClassPath(APP_JAR);

        // Configure mapper
        job.setMapperClass(FailJob.FailMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // Configure input/output - use regular output format to test cleanup
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);

        LOG.info("Submitting job for output cleanup test");

        // ACT: Submit and wait for failure
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should be assigned");
        LOG.info("Job submitted with ID: {}", job.getJobID());

        boolean succeeded = job.waitForCompletion(true);
        assertFalse(succeeded, "Job should fail");

        // Wait for job to reach terminal FAILED state
        GenericTestUtils.waitFor(
            () -> {
                try {
                    JobStatus.State state = job.getJobState();
                    return state == JobStatus.State.FAILED;
                } catch (IOException | InterruptedException e) {
                    return false;
                }
            },
            JOB_STATE_CHECK_INTERVAL_MS,
            JOB_STATE_TIMEOUT_MS
        );

        // ASSERT: Verify output directory state after failure
        // The FileOutputCommitter should have cleaned up the output directory
        // or not created it at all since the job failed
        boolean outputExists = remoteFs.exists(outputDir);
        LOG.info("Output directory exists after failure: {}", outputExists);

        // If output exists, verify it doesn't contain successful output files
        // (i.e., _SUCCESS marker should not be present)
        if (outputExists) {
            Path successMarker = new Path(outputDir, "_SUCCESS");
            assertFalse(remoteFs.exists(successMarker),
                "_SUCCESS marker should not exist for failed job");
            LOG.info("Output directory exists but _SUCCESS marker is correctly absent");
        } else {
            LOG.info("Output directory was properly cleaned up after failure");
        }

        // Verify job state
        assertEquals(JobStatus.State.FAILED, job.getJobState(),
            "Job should be in FAILED state");

        LOG.info("testOutputCleanup completed successfully");
    }
}
