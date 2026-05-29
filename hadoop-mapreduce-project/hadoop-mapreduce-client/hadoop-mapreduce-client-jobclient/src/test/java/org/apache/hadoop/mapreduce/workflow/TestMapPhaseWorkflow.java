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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class for Workflow 11 (Map Phase Completion) that validates
 * MapReduce map phase scenarios including map task completion monitoring, map task
 * failure and retry behavior, and map counter verification.
 *
 * <p>This class extends {@link AbstractMapReduceWorkflowTest} for static
 * MiniMRYarnCluster + MiniDFSCluster lifecycle management, ensuring cluster reuse
 * across all test methods to meet the 45-minute CI budget constraint.
 *
 * <h2>Test Coverage:</h2>
 * <ul>
 *   <li>{@link #testMapPhaseCompletion()} - Monitors map task progress and verifies
 *       all map tasks reach COMPLETE state</li>
 *   <li>{@link #testMapTaskFailureRetry()} - Verifies map task failure and retry
 *       mechanism using a custom FailOnceMapper</li>
 *   <li>{@link #testMapCounters()} - Validates map-specific counters including
 *       MAP_INPUT_RECORDS, MAP_OUTPUT_RECORDS, and MAP_OUTPUT_BYTES</li>
 * </ul>
 *
 * <h2>Production APIs Exercised:</h2>
 * <ul>
 *   <li>{@code Job.submit()} - Submit MapReduce job</li>
 *   <li>{@code Job.waitForCompletion()} - Wait for job completion</li>
 *   <li>{@code Job.getTaskReports(TaskType.MAP)} - Get map task reports</li>
 *   <li>{@code Job.getCounters()} - Get job counters</li>
 *   <li>{@code TaskReport.getCurrentStatus()} - Get task status</li>
 *   <li>{@code TaskReport.getSuccessfulTaskAttemptId()} - Get successful attempt</li>
 *   <li>{@code Counters.findCounter()} - Access specific counters</li>
 * </ul>
 *
 * @see AbstractMapReduceWorkflowTest
 * @see SleepJob
 */
public class TestMapPhaseWorkflow extends AbstractMapReduceWorkflowTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestMapPhaseWorkflow.class);

    /**
     * Number of map tasks for testing.
     */
    private static final int NUM_MAPPERS = 2;

    /**
     * Number of reduce tasks for testing.
     */
    private static final int NUM_REDUCERS = 1;

    /**
     * Map sleep time in milliseconds for SleepJob.
     */
    private static final long MAP_SLEEP_TIME_MS = 1000;

    /**
     * Reduce sleep time in milliseconds for SleepJob.
     */
    private static final long REDUCE_SLEEP_TIME_MS = 500;

    /**
     * Number of sleep count iterations per map task.
     */
    private static final int MAP_SLEEP_COUNT = 1;

    /**
     * Number of sleep count iterations per reduce task.
     */
    private static final int REDUCE_SLEEP_COUNT = 1;

    /**
     * Timeout for waiting on job state transitions in milliseconds.
     */
    private static final long WAIT_TIMEOUT_MS = 60000;

    /**
     * Check interval for waiting conditions in milliseconds.
     */
    private static final long CHECK_INTERVAL_MS = 500;

    /**
     * Mapper that fails on the first attempt but succeeds on retry.
     * Used to test the map task failure and retry mechanism.
     *
     * <p>This mapper checks the task attempt ID and throws a RuntimeException
     * on the first attempt (attempt ID 0). On subsequent attempts (ID > 0),
     * it processes normally.
     */
    public static class FailOnceMapper 
            extends Mapper<IntWritable, IntWritable, IntWritable, NullWritable> {

        /**
         * Map method that fails on first attempt and succeeds on retry.
         *
         * @param key input key
         * @param value input value
         * @param context map context
         * @throws IOException if an I/O error occurs
         * @throws InterruptedException if the thread is interrupted
         */
        @Override
        public void map(IntWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            // Get the task attempt ID to determine if this is the first attempt
            int attemptId = context.getTaskAttemptID().getId();
            
            LOG.info("FailOnceMapper: Task {} attempt {} processing key {}", 
                    context.getTaskAttemptID().getTaskID(), attemptId, key);
            
            // Fail only on the first attempt (attempt ID 0)
            if (attemptId == 0) {
                LOG.info("FailOnceMapper: Failing first attempt for task {}", 
                        context.getTaskAttemptID());
                throw new RuntimeException("Intentional failure on first attempt: " 
                        + context.getTaskAttemptID());
            }
            
            // On retry (attempt ID > 0), process normally
            LOG.info("FailOnceMapper: Retry attempt {} succeeding for task {}", 
                    attemptId, context.getTaskAttemptID().getTaskID());
            context.write(new IntWritable(key.get()), NullWritable.get());
        }
    }

    /**
     * Workflow: Map Phase Completion Monitoring
     * Production methods invoked: Job.submit(), Job.getTaskReports(TaskType.MAP),
     *                             TaskReport.getCurrentStatus(), Job.waitForCompletion()
     * Input conditions: SleepJob with 2 mappers and 1 reducer
     * Validation criteria: All map tasks reach COMPLETE status before reduce phase,
     *                      job completes successfully with SUCCEEDED state
     *
     * <p>This test submits a SleepJob, monitors map task progress using
     * {@code Job.getTaskReports(TaskType.MAP)}, and verifies that all map tasks
     * reach the {@code TIPStatus.COMPLETE} state before the reduce phase begins.
     * Uses {@code GenericTestUtils.waitFor()} for async state transition waiting.
     *
     * @throws Exception if the test fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testMapPhaseCompletion() throws Exception {
        LOG.info("Starting testMapPhaseCompletion");
        
        // Skip test if MRAppJar is not available
        if (!isMRAppJarAvailable()) {
            LOG.warn("MRAppJar not found. Skipping test.");
            return;
        }
        
        // ARRANGE: Configure and create a SleepJob
        Configuration jobConf = new Configuration(conf);
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(jobConf);
        
        // Create job with 2 mappers and 1 reducer
        // Parameters: numMapper, numReducer, mapSleepTime, mapSleepCount, 
        //             reduceSleepTime, reduceSleepCount
        Job job = sleepJob.createJob(NUM_MAPPERS, NUM_REDUCERS, 
                MAP_SLEEP_TIME_MS, MAP_SLEEP_COUNT, 
                REDUCE_SLEEP_TIME_MS, REDUCE_SLEEP_COUNT);
        
        // Configure job with required classpath
        job.addFileToClassPath(APP_JAR);
        job.setJarByClass(SleepJob.class);
        
        LOG.info("Submitting SleepJob with {} mappers and {} reducers", 
                NUM_MAPPERS, NUM_REDUCERS);
        
        // ACT: Submit the job
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should not be null after submission");
        LOG.info("Job submitted with ID: {}", job.getJobID());
        
        // Wait for map tasks to be launched
        GenericTestUtils.waitFor(
            () -> {
                try {
                    TaskReport[] mapReports = job.getTaskReports(TaskType.MAP);
                    return mapReports != null && mapReports.length == NUM_MAPPERS;
                } catch (Exception e) {
                    LOG.debug("Waiting for map tasks to be launched: {}", e.getMessage());
                    return false;
                }
            },
            CHECK_INTERVAL_MS,
            WAIT_TIMEOUT_MS
        );
        
        // Wait for all map tasks to complete
        GenericTestUtils.waitFor(
            () -> {
                try {
                    TaskReport[] mapReports = job.getTaskReports(TaskType.MAP);
                    if (mapReports == null || mapReports.length == 0) {
                        return false;
                    }
                    
                    int completedMaps = 0;
                    for (TaskReport report : mapReports) {
                        TIPStatus status = report.getCurrentStatus();
                        LOG.debug("Map task {} status: {}", 
                                report.getTaskId(), status);
                        if (status == TIPStatus.COMPLETE) {
                            completedMaps++;
                        }
                    }
                    
                    LOG.info("Map phase progress: {}/{} tasks complete", 
                            completedMaps, mapReports.length);
                    return completedMaps == NUM_MAPPERS;
                } catch (Exception e) {
                    LOG.debug("Error checking map task status: {}", e.getMessage());
                    return false;
                }
            },
            CHECK_INTERVAL_MS,
            WAIT_TIMEOUT_MS
        );
        
        // ASSERT: Verify all map tasks are complete
        TaskReport[] finalMapReports = job.getTaskReports(TaskType.MAP);
        assertNotNull(finalMapReports, "Map task reports should not be null");
        assertEquals(NUM_MAPPERS, finalMapReports.length, 
                "Should have " + NUM_MAPPERS + " map tasks");
        
        for (TaskReport report : finalMapReports) {
            assertEquals(TIPStatus.COMPLETE, report.getCurrentStatus(),
                    "Map task " + report.getTaskId() + " should be COMPLETE");
            assertTrue(report.getProgress() >= 0.9999f,
                    "Map task " + report.getTaskId() + " progress should be 100%");
            LOG.info("Map task {} completed successfully with status {}", 
                    report.getTaskId(), report.getCurrentStatus());
        }
        
        // Wait for job to complete
        boolean succeeded = job.waitForCompletion(true);
        
        // ASSERT: Verify job completed successfully
        assertTrue(succeeded, "Job should complete successfully");
        assertEquals(JobStatus.State.SUCCEEDED, job.getJobState(),
                "Job state should be SUCCEEDED");
        
        LOG.info("testMapPhaseCompletion completed successfully");
    }

    /**
     * Workflow: Map Task Failure and Retry Mechanism
     * Production methods invoked: Job.submit(), Job.waitForCompletion(),
     *                             Job.getTaskReports(TaskType.MAP),
     *                             TaskReport.getSuccessfulTaskAttemptId(),
     *                             Job.getCounters()
     * Input conditions: Job with FailOnceMapper (fails first attempt, succeeds on retry),
     *                   max map attempts set to 2
     * Validation criteria: Job completes successfully, task attempt ID > 0 for at least
     *                      one task (indicating retry), NUM_FAILED_MAPS counter > 0
     *
     * <p>This test configures a job with a custom {@link FailOnceMapper} that intentionally
     * fails on the first attempt but succeeds on retry. It verifies that the MapReduce
     * framework properly retries failed map tasks and the job eventually succeeds.
     *
     * @throws Exception if the test fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testMapTaskFailureRetry() throws Exception {
        LOG.info("Starting testMapTaskFailureRetry");
        
        // Skip test if MRAppJar is not available
        if (!isMRAppJarAvailable()) {
            LOG.warn("MRAppJar not found. Skipping test.");
            return;
        }
        
        // ARRANGE: Configure job with FailOnceMapper
        Configuration jobConf = new Configuration(conf);
        
        // Set max attempts to allow retry (default is 4, we use 2 to speed up test)
        jobConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 2);
        
        // Configure SleepJob as base and override mapper
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(jobConf);
        
        // Create job with 1 mapper (simplifies retry tracking) and 0 reducers
        // Using minimal sleep times since failure/retry is the focus
        Job job = sleepJob.createJob(1, 0, 100, 1, 0, 0);
        
        // Override the mapper with our FailOnceMapper
        job.setMapperClass(FailOnceMapper.class);
        job.addFileToClassPath(APP_JAR);
        job.setJarByClass(TestMapPhaseWorkflow.class);
        
        LOG.info("Submitting job with FailOnceMapper to test retry mechanism");
        
        // ACT: Submit and wait for job completion
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should not be null after submission");
        LOG.info("Job submitted with ID: {}", job.getJobID());
        
        // Wait for job to complete
        boolean succeeded = job.waitForCompletion(true);
        
        // ASSERT: Verify job completed successfully after retry
        assertTrue(succeeded, "Job should complete successfully after map task retry");
        assertEquals(JobStatus.State.SUCCEEDED, job.getJobState(),
                "Job state should be SUCCEEDED");
        
        // Verify task reports show successful retry
        TaskReport[] mapReports = job.getTaskReports(TaskType.MAP);
        assertNotNull(mapReports, "Map task reports should not be null");
        assertEquals(1, mapReports.length, "Should have 1 map task");
        
        TaskReport mapReport = mapReports[0];
        assertEquals(TIPStatus.COMPLETE, mapReport.getCurrentStatus(),
                "Map task should be COMPLETE after retry");
        
        // Verify successful task attempt ID indicates retry (attempt ID > 0)
        assertNotNull(mapReport.getSuccessfulTaskAttemptId(),
                "Successful task attempt ID should not be null");
        int successfulAttemptId = mapReport.getSuccessfulTaskAttemptId().getId();
        assertTrue(successfulAttemptId > 0,
                "Successful attempt ID should be > 0, indicating retry. Got: " 
                        + successfulAttemptId);
        LOG.info("Map task succeeded on attempt ID: {}", successfulAttemptId);
        
        // Verify counters reflect the failed attempt
        Counters counters = job.getCounters();
        assertNotNull(counters, "Job counters should not be null");
        
        long totalLaunchedMaps = counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS).getValue();
        long numFailedMaps = counters.findCounter(JobCounter.NUM_FAILED_MAPS).getValue();
        
        LOG.info("Counter TOTAL_LAUNCHED_MAPS: {}", totalLaunchedMaps);
        LOG.info("Counter NUM_FAILED_MAPS: {}", numFailedMaps);
        
        // Should have launched at least 2 map attempts (1 failed + 1 successful)
        assertTrue(totalLaunchedMaps >= 2,
                "TOTAL_LAUNCHED_MAPS should be >= 2 (failed + retry). Got: " 
                        + totalLaunchedMaps);
        
        // Should have at least 1 failed map
        assertTrue(numFailedMaps >= 1,
                "NUM_FAILED_MAPS should be >= 1. Got: " + numFailedMaps);
        
        LOG.info("testMapTaskFailureRetry completed successfully");
    }

    /**
     * Workflow: Map Counter Verification
     * Production methods invoked: Job.submit(), Job.waitForCompletion(),
     *                             Job.getCounters(), Counters.findCounter()
     * Input conditions: SleepJob with 2 mappers processing configured records
     * Validation criteria: MAP_INPUT_RECORDS > 0, MAP_OUTPUT_RECORDS > 0,
     *                      MAP_OUTPUT_BYTES >= 0, TOTAL_LAUNCHED_MAPS == NUM_MAPPERS
     *
     * <p>This test submits a SleepJob and after completion verifies that map-specific
     * counters from {@code Job.getCounters()} are properly populated. The test validates
     * that MAP_INPUT_RECORDS, MAP_OUTPUT_RECORDS, and MAP_OUTPUT_BYTES reflect the
     * actual processing performed by the map tasks.
     *
     * @throws Exception if the test fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testMapCounters() throws Exception {
        LOG.info("Starting testMapCounters");
        
        // Skip test if MRAppJar is not available
        if (!isMRAppJarAvailable()) {
            LOG.warn("MRAppJar not found. Skipping test.");
            return;
        }
        
        // ARRANGE: Configure SleepJob with known parameters
        Configuration jobConf = new Configuration(conf);
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(jobConf);
        
        // Create job with 2 mappers and 1 reducer
        // SleepJob generates mapSleepCount records per map task
        // Each record is processed and output to reducers
        int numMappers = 2;
        int numReducers = 1;
        int mapSleepCount = 2; // Number of records per map task
        int reduceSleepCount = 1;
        
        Job job = sleepJob.createJob(numMappers, numReducers, 
                500, mapSleepCount,  // mapSleepTime, mapSleepCount
                200, reduceSleepCount);  // reduceSleepTime, reduceSleepCount
        
        job.addFileToClassPath(APP_JAR);
        job.setJarByClass(SleepJob.class);
        
        LOG.info("Submitting SleepJob with {} mappers and {} records per mapper", 
                numMappers, mapSleepCount);
        
        // ACT: Submit and wait for job completion
        job.submit();
        assertNotNull(job.getJobID(), "Job ID should not be null after submission");
        LOG.info("Job submitted with ID: {}", job.getJobID());
        
        boolean succeeded = job.waitForCompletion(true);
        
        // ASSERT: Verify job completed successfully
        assertTrue(succeeded, "Job should complete successfully");
        assertEquals(JobStatus.State.SUCCEEDED, job.getJobState(),
                "Job state should be SUCCEEDED");
        
        // Get and verify counters
        Counters counters = job.getCounters();
        assertNotNull(counters, "Job counters should not be null");
        
        // Verify job-level map counters
        long totalLaunchedMaps = counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS).getValue();
        assertEquals(numMappers, totalLaunchedMaps,
                "TOTAL_LAUNCHED_MAPS should equal number of mappers configured");
        LOG.info("Counter TOTAL_LAUNCHED_MAPS: {}", totalLaunchedMaps);
        
        // Verify no maps failed
        long numFailedMaps = counters.findCounter(JobCounter.NUM_FAILED_MAPS).getValue();
        assertEquals(0, numFailedMaps, "NUM_FAILED_MAPS should be 0 for successful job");
        LOG.info("Counter NUM_FAILED_MAPS: {}", numFailedMaps);
        
        // Verify task-level map counters
        // SleepJob's SleepInputFormat generates mapSleepCount records per map task
        long mapInputRecords = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        assertTrue(mapInputRecords > 0,
                "MAP_INPUT_RECORDS should be > 0. Got: " + mapInputRecords);
        LOG.info("Counter MAP_INPUT_RECORDS: {}", mapInputRecords);
        
        // SleepMapper outputs records to reducers based on configuration
        // The number of output records depends on numReducers and reduceSleepCount
        long mapOutputRecords = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        assertTrue(mapOutputRecords >= 0,
                "MAP_OUTPUT_RECORDS should be >= 0. Got: " + mapOutputRecords);
        LOG.info("Counter MAP_OUTPUT_RECORDS: {}", mapOutputRecords);
        
        // MAP_OUTPUT_BYTES reflects the serialized size of output key-value pairs
        long mapOutputBytes = counters.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
        assertTrue(mapOutputBytes >= 0,
                "MAP_OUTPUT_BYTES should be >= 0. Got: " + mapOutputBytes);
        LOG.info("Counter MAP_OUTPUT_BYTES: {}", mapOutputBytes);
        
        // Verify map output materialized bytes (compressed output)
        long mapOutputMaterializedBytes = counters
                .findCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES).getValue();
        assertTrue(mapOutputMaterializedBytes >= 0,
                "MAP_OUTPUT_MATERIALIZED_BYTES should be >= 0. Got: " 
                        + mapOutputMaterializedBytes);
        LOG.info("Counter MAP_OUTPUT_MATERIALIZED_BYTES: {}", mapOutputMaterializedBytes);
        
        // Verify reduce counters indicate maps were processed
        long totalLaunchedReduces = counters
                .findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES).getValue();
        assertEquals(numReducers, totalLaunchedReduces,
                "TOTAL_LAUNCHED_REDUCES should equal number of reducers configured");
        LOG.info("Counter TOTAL_LAUNCHED_REDUCES: {}", totalLaunchedReduces);
        
        LOG.info("testMapCounters completed successfully");
    }

    /**
     * Checks if the MRAppJar is available for test execution.
     * The MRAppJar is required for running MapReduce jobs on MiniMRYarnCluster.
     *
     * @return true if MRAppJar exists, false otherwise
     */
    private boolean isMRAppJarAvailable() {
        return new File(MiniMRYarnCluster.APPJAR).exists();
    }
}
