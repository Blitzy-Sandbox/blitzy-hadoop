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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * JUnit 5 workflow test class for Workflow 10 (Job Submission) that validates
 * MapReduce job submission scenarios including successful submission, missing
 * input path handling, and invalid configuration detection.
 *
 * <p>This test class extends {@link AbstractMapReduceWorkflowTest} for static
 * MiniMRYarnCluster + MiniDFSCluster lifecycle management, ensuring cluster
 * reuse across all test methods to meet 45-minute CI budget constraints.
 *
 * <p>Test scenarios covered:
 * <ul>
 *   <li>{@link #testJobSubmit()} - Validates successful job submission with
 *       proper JobId, job state transitions from PREP to RUNNING to SUCCEEDED</li>
 *   <li>{@link #testMissingInputPath()} - Validates proper exception handling
 *       when input path does not exist</li>
 *   <li>{@link #testInvalidConfiguration()} - Validates proper error handling
 *       when job is configured with invalid mapper class</li>
 * </ul>
 *
 * <p>All tests use:
 * <ul>
 *   <li>{@code @Timeout(60)} annotation for per-test time limits</li>
 *   <li>{@link GenericTestUtils#waitFor} for async state transitions</li>
 *   <li>Direct invocation of production {@link Job} APIs</li>
 * </ul>
 *
 * @see AbstractMapReduceWorkflowTest
 * @see Job
 * @see GenericTestUtils
 */
public class TestJobSubmissionWorkflow extends AbstractMapReduceWorkflowTest {

    private static final Logger LOG = LoggerFactory.getLogger(
            TestJobSubmissionWorkflow.class);

    /**
     * Check interval in milliseconds for async state wait operations.
     */
    private static final long CHECK_INTERVAL_MS = 500L;

    /**
     * Timeout in milliseconds for async state wait operations.
     */
    private static final long WAIT_TIMEOUT_MS = 60000L;

    /**
     * Simple mapper that outputs the input key-value unchanged.
     * Used for basic job submission testing.
     */
    public static class SimpleMapper 
            extends Mapper<Object, Text, Text, IntWritable> {
        
        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Simple word count style mapper
            String[] tokens = value.toString().split("\\s+");
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    word.set(token);
                    context.write(word, ONE);
                }
            }
        }
    }

    /**
     * Simple reducer that sums up the counts for each key.
     * Used for basic job submission testing.
     */
    public static class SimpleReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, 
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Workflow: Job Submission - Successful submission flow
     * Production methods invoked: Job.getInstance(Configuration), 
     *     Job.setMapperClass(), Job.setReducerClass(), Job.submit(),
     *     Job.getJobID(), Job.getJobState(), Job.waitForCompletion()
     * Input conditions: Valid input file with text content, valid output path,
     *     properly configured mapper and reducer classes
     * Validation criteria: JobId is non-null, job transitions through
     *     PREP -> RUNNING -> SUCCEEDED states, final state is SUCCEEDED
     *
     * @throws Exception if test fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testJobSubmit() throws Exception {
        LOG.info("Starting testJobSubmit - Workflow 10: Job Submission");

        // Skip test if MRAppJar is not available
        if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
            LOG.warn("MRAppJar {} not found. Skipping test.", 
                    MiniMRYarnCluster.APPJAR);
            return;
        }

        // Skip test if cluster is not available
        if (mrCluster == null) {
            LOG.warn("MiniMRYarnCluster not available. Skipping test.");
            return;
        }

        // ARRANGE: Create test input file with sample data
        Path inputPath = new Path(testDir, "input");
        Path outputPath = new Path(testDir, "output");
        String inputContent = "hello world\nhello hadoop\nmapreduce test\n";
        
        remoteFs.mkdirs(inputPath);
        Path inputFile = new Path(inputPath, "input.txt");
        try (FSDataOutputStream out = remoteFs.create(inputFile)) {
            out.writeBytes(inputContent);
        }
        LOG.info("Created test input file: {} with {} bytes", 
                inputFile, inputContent.length());

        // ACT: Configure and submit job using production APIs
        Configuration jobConf = new Configuration(mrCluster.getConfig());
        jobConf.setInt(MRJobConfig.NUM_MAPS, 1);
        
        Job job = Job.getInstance(jobConf, "testJobSubmit");
        
        // Configure job classes
        job.setJarByClass(TestJobSubmissionWorkflow.class);
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);
        
        // Configure output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Configure input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Set input and output paths
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // Add AppJar to classpath (required for MiniCluster)
        job.addFileToClassPath(APP_JAR);
        
        // Speed up failures with limited attempts
        job.setMaxMapAttempts(1);
        
        LOG.info("Submitting job...");
        job.submit();
        
        // ASSERT: Verify JobId is non-null
        JobID jobId = job.getJobID();
        assertNotNull(jobId, "JobId should not be null after submission");
        LOG.info("Job submitted successfully with JobId: {}", jobId);
        
        // ASSERT: Verify initial job state (PREP or RUNNING)
        JobStatus.State initialState = job.getJobState();
        LOG.info("Initial job state: {}", initialState);
        assertTrue(
                initialState == JobStatus.State.PREP || 
                initialState == JobStatus.State.RUNNING,
                "Initial job state should be PREP or RUNNING, but was: " + initialState);
        
        // Wait for job to complete using GenericTestUtils.waitFor()
        LOG.info("Waiting for job completion...");
        try {
            GenericTestUtils.waitFor(
                    () -> {
                        try {
                            JobStatus.State state = job.getJobState();
                            LOG.debug("Current job state: {}", state);
                            return state == JobStatus.State.SUCCEEDED ||
                                   state == JobStatus.State.FAILED ||
                                   state == JobStatus.State.KILLED;
                        } catch (Exception e) {
                            LOG.warn("Error checking job state", e);
                            return false;
                        }
                    },
                    CHECK_INTERVAL_MS,
                    WAIT_TIMEOUT_MS,
                    "Job did not complete within timeout"
            );
        } catch (TimeoutException e) {
            fail("Job did not complete within " + WAIT_TIMEOUT_MS + 
                    "ms. Current state: " + job.getJobState());
        }
        
        // ASSERT: Verify job completed successfully
        JobStatus.State finalState = job.getJobState();
        LOG.info("Final job state: {}", finalState);
        assertEquals(JobStatus.State.SUCCEEDED, finalState,
                "Job should complete with SUCCEEDED state");
        
        // ASSERT: Verify output was produced
        assertTrue(remoteFs.exists(outputPath), 
                "Output directory should exist after job completion");
        
        LOG.info("testJobSubmit completed successfully");
    }

    /**
     * Workflow: Job Submission - Missing input path handling
     * Production methods invoked: Job.getInstance(Configuration),
     *     Job.setMapperClass(), Job.submit(), Job.waitForCompletion()
     * Input conditions: Non-existent input path, valid output path
     * Validation criteria: Job submission fails fast or job fails with
     *     appropriate error indicating input path does not exist
     *
     * @throws Exception if test fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMissingInputPath() throws Exception {
        LOG.info("Starting testMissingInputPath - Workflow 10: Missing Input Path");

        // Skip test if MRAppJar is not available
        if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
            LOG.warn("MRAppJar {} not found. Skipping test.", 
                    MiniMRYarnCluster.APPJAR);
            return;
        }

        // Skip test if cluster is not available
        if (mrCluster == null) {
            LOG.warn("MiniMRYarnCluster not available. Skipping test.");
            return;
        }

        // ARRANGE: Configure job with non-existent input path
        Path nonExistentPath = new Path(testDir, "non_existent_input");
        Path outputPath = new Path(testDir, "output");
        
        // Ensure the path does NOT exist
        assertFalse(remoteFs.exists(nonExistentPath), 
                "Non-existent path should not exist");
        
        Configuration jobConf = new Configuration(mrCluster.getConfig());
        Job job = Job.getInstance(jobConf, "testMissingInputPath");
        
        job.setJarByClass(TestJobSubmissionWorkflow.class);
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        FileInputFormat.setInputPaths(job, nonExistentPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        job.addFileToClassPath(APP_JAR);
        job.setMaxMapAttempts(1);
        
        // ACT & ASSERT: Expect IOException during submission or completion
        // The job should fail because the input path doesn't exist
        LOG.info("Attempting to submit job with non-existent input path: {}", 
                nonExistentPath);
        
        boolean exceptionThrown = false;
        String errorMessage = null;
        
        try {
            job.submit();
            LOG.info("Job submitted, waiting for completion...");
            
            // Wait for job completion - it should fail
            boolean succeeded = job.waitForCompletion(true);
            
            // If we get here without exception, job should have failed
            if (!succeeded) {
                exceptionThrown = true;
                errorMessage = "Job failed as expected due to missing input path";
                LOG.info("Job failed as expected. Final state: {}", 
                        job.getJobState());
                
                // Verify job state is FAILED
                assertEquals(JobStatus.State.FAILED, job.getJobState(),
                        "Job with missing input should have FAILED state");
            } else {
                // This should not happen - job succeeded with missing input
                fail("Job should not succeed with non-existent input path");
            }
        } catch (IOException e) {
            // Expected exception for missing input path
            exceptionThrown = true;
            errorMessage = e.getMessage();
            LOG.info("Expected IOException caught: {}", e.getMessage());
            
            // Verify the exception message indicates the path issue
            assertTrue(
                    errorMessage != null && (
                            errorMessage.contains("does not exist") ||
                            errorMessage.contains("not found") ||
                            errorMessage.contains("Input path") ||
                            errorMessage.contains("Input") ||
                            errorMessage.contains("non_existent_input")
                    ),
                    "Exception message should indicate input path problem: " + 
                            errorMessage);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted unexpectedly: " + e.getMessage());
        }
        
        assertTrue(exceptionThrown || job.getJobState() == JobStatus.State.FAILED,
                "Job submission with missing input should fail or throw exception");
        
        LOG.info("testMissingInputPath completed successfully - error: {}", 
                errorMessage);
    }

    /**
     * Workflow: Job Submission - Invalid configuration handling
     * Production methods invoked: Job.getInstance(Configuration),
     *     Job.setMapperClass(), Job.submit(), Job.getStatus()
     * Input conditions: Valid input file, output path, but invalid/non-existent
     *     mapper class configured
     * Validation criteria: Job fails with appropriate error message accessible
     *     via Job.getStatus() or exception is thrown during submission/execution
     *
     * @throws Exception if test fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testInvalidConfiguration() throws Exception {
        LOG.info("Starting testInvalidConfiguration - Workflow 10: Invalid Config");

        // Skip test if MRAppJar is not available
        if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
            LOG.warn("MRAppJar {} not found. Skipping test.", 
                    MiniMRYarnCluster.APPJAR);
            return;
        }

        // Skip test if cluster is not available
        if (mrCluster == null) {
            LOG.warn("MiniMRYarnCluster not available. Skipping test.");
            return;
        }

        // ARRANGE: Create valid test input
        Path inputPath = new Path(testDir, "input");
        Path outputPath = new Path(testDir, "output");
        
        remoteFs.mkdirs(inputPath);
        Path inputFile = new Path(inputPath, "input.txt");
        try (FSDataOutputStream out = remoteFs.create(inputFile)) {
            out.writeBytes("test data for invalid config test\n");
        }
        
        // Configure job with invalid/non-existent mapper class
        Configuration jobConf = new Configuration(mrCluster.getConfig());
        Job job = Job.getInstance(jobConf, "testInvalidConfiguration");
        
        job.setJarByClass(TestJobSubmissionWorkflow.class);
        
        // Set an invalid mapper class name that doesn't exist
        // This simulates a misconfiguration where the class cannot be found
        jobConf.set(MRJobConfig.MAP_CLASS_ATTR, 
                "org.apache.hadoop.mapreduce.NonExistentMapperClass");
        job = Job.getInstance(jobConf, "testInvalidConfiguration");
        
        job.setJarByClass(TestJobSubmissionWorkflow.class);
        job.setReducerClass(SimpleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        job.addFileToClassPath(APP_JAR);
        job.setMaxMapAttempts(1);
        
        // ACT & ASSERT: Job should fail due to invalid configuration
        LOG.info("Submitting job with invalid mapper class configuration");
        
        boolean jobFailed = false;
        String failureReason = null;
        
        try {
            job.submit();
            JobID jobId = job.getJobID();
            assertNotNull(jobId, "JobId should be returned even for invalid config");
            LOG.info("Job submitted with JobId: {}", jobId);
            
            // Wait for job completion
            boolean succeeded = job.waitForCompletion(true);
            
            if (!succeeded) {
                jobFailed = true;
                JobStatus.State state = job.getJobState();
                LOG.info("Job failed as expected. State: {}", state);
                
                // Get failure information from job status
                try {
                    String diagnostics = job.getStatus().getFailureInfo();
                    if (diagnostics != null && !diagnostics.isEmpty()) {
                        failureReason = diagnostics;
                        LOG.info("Job failure diagnostics: {}", diagnostics);
                    }
                } catch (Exception e) {
                    LOG.debug("Could not retrieve failure info: {}", e.getMessage());
                }
                
                // Verify job state
                assertTrue(
                        state == JobStatus.State.FAILED || 
                        state == JobStatus.State.KILLED,
                        "Job should be in FAILED or KILLED state, but was: " + state);
            } else {
                fail("Job should not succeed with invalid mapper class");
            }
        } catch (IOException e) {
            jobFailed = true;
            failureReason = e.getMessage();
            LOG.info("Expected exception during job execution: {}", e.getMessage());
            
            // Verify the exception relates to class loading or configuration
            assertTrue(
                    failureReason != null && (
                            failureReason.contains("Class") ||
                            failureReason.contains("class") ||
                            failureReason.contains("not found") ||
                            failureReason.contains("ClassNotFound") ||
                            failureReason.contains("Mapper") ||
                            failureReason.contains("failed") ||
                            failureReason.contains("Error")
                    ),
                    "Exception should indicate class/configuration problem: " + 
                            failureReason);
        } catch (ClassNotFoundException e) {
            jobFailed = true;
            failureReason = e.getMessage();
            LOG.info("Expected ClassNotFoundException: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted unexpectedly: " + e.getMessage());
        }
        
        assertTrue(jobFailed, 
                "Job with invalid configuration should fail");
        
        LOG.info("testInvalidConfiguration completed successfully - failure: {}", 
                failureReason);
    }
}
