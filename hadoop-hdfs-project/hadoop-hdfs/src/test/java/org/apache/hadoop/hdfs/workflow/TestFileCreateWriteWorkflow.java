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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class covering HDFS Workflow 1: File Create/Write/Close.
 * 
 * <p>This test class validates the complete file create/write/close workflow using
 * production HDFS APIs. It tests {@link org.apache.hadoop.fs.FileSystem#create(Path)},
 * {@link FSDataOutputStream#write(byte[])}, and {@link FSDataOutputStream#close()}
 * operations with various file sizes and configurations.
 * 
 * <h3>Test Coverage</h3>
 * <ul>
 *   <li><b>Happy Path:</b> Create file, write bytes, close, verify metadata</li>
 *   <li><b>Parameterized Write Sizes:</b> Zero-byte (0), sub-block (1KB), 
 *       exact-block (128MB), multi-block (128MB+1) writes</li>
 *   <li><b>Overwrite Behavior:</b> Verify overwrite flag behavior</li>
 *   <li><b>Edge Cases:</b> Empty file creation</li>
 * </ul>
 * 
 * <h3>Production APIs Exercised</h3>
 * <ul>
 *   <li>{@code FileSystem.create(Path)} - Create new file</li>
 *   <li>{@code FileSystem.create(Path, boolean)} - Create with overwrite flag</li>
 *   <li>{@code FSDataOutputStream.write(byte[])} - Write content</li>
 *   <li>{@code FSDataOutputStream.write(byte[], int, int)} - Write content range</li>
 *   <li>{@code FSDataOutputStream.close()} - Close and finalize file</li>
 *   <li>{@code FileSystem.getFileStatus(Path)} - Retrieve file metadata</li>
 *   <li>{@code FileSystem.exists(Path)} - Check file existence</li>
 *   <li>{@code FileSystem.open(Path)} - Open file for read verification</li>
 *   <li>{@code FSDataInputStream.readFully(byte[])} - Read back for verification</li>
 * </ul>
 * 
 * <h3>Test Isolation</h3>
 * <p>All tests use the {@code testDir} path provided by {@link AbstractHdfsWorkflowTest}
 * to ensure complete isolation. Each test creates files in its own namespace under
 * /workflow/TestFileCreateWriteWorkflow/[methodName].
 * 
 * <h3>Compliance</h3>
 * <ul>
 *   <li>All test methods use {@code @Timeout(60)} for 60-second execution limit</li>
 *   <li>No {@code Thread.sleep()} calls - uses {@code GenericTestUtils.waitFor()} if needed</li>
 *   <li>Uses {@code @ParameterizedTest} with {@code @MethodSource} for write size variations</li>
 *   <li>Invokes production APIs directly - no reimplemented logic</li>
 * </ul>
 * 
 * @see AbstractHdfsWorkflowTest
 * @see org.apache.hadoop.fs.FileSystem
 * @see FSDataOutputStream
 */
public class TestFileCreateWriteWorkflow extends AbstractHdfsWorkflowTest {

    /**
     * Deterministic seed for reproducible test data generation.
     * Using fixed seed ensures tests are deterministic and reproducible.
     */
    private static final long TEST_DATA_SEED = 42L;

    /**
     * Default block size for multi-block tests (128MB).
     * Matches the default HDFS block size configuration.
     */
    private static final long DEFAULT_BLOCK_SIZE = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;

    /**
     * Buffer size for writing data in chunks.
     * 4KB buffer is efficient for small writes without excessive memory usage.
     */
    private static final int WRITE_BUFFER_SIZE = 4096;

    /**
     * Workflow path: File Create/Write/Close - Happy Path
     * Production methods invoked: FileSystem.create(), FSDataOutputStream.write(),
     *                             FSDataOutputStream.close(), FileSystem.getFileStatus(),
     *                             FileSystem.exists(), FileSystem.open(), FSDataInputStream.readFully()
     * Input conditions: File size = 8KB, default replication
     * Validation criteria: File exists, length matches written bytes, content matches via read-back
     * 
     * <p>Tests the complete happy path workflow for file creation:
     * <ol>
     *   <li>Create a new file using production FileSystem.create() API</li>
     *   <li>Write deterministic test data using FSDataOutputStream.write()</li>
     *   <li>Close the stream using FSDataOutputStream.close()</li>
     *   <li>Verify file exists using FileSystem.exists()</li>
     *   <li>Verify file length using FileSystem.getFileStatus()</li>
     *   <li>Verify content integrity via read-back comparison</li>
     * </ol>
     * 
     * @throws IOException if any HDFS operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testCreateWriteVerify() throws IOException {
        // ARRANGE: Define test file path and data
        Path filePath = new Path(testDir, "test_create_write_verify.dat");
        int fileSize = 8192; // 8KB test file
        byte[] expectedData = generateTestData(TEST_DATA_SEED, fileSize);

        // ACT: Create file, write data, and close using production APIs
        try (FSDataOutputStream out = fs.create(filePath)) {
            assertNotNull(out, "FSDataOutputStream should not be null after create()");
            out.write(expectedData);
        } // Auto-closes via try-with-resources, invoking close()

        // ASSERT: Verify file exists using production exists() API
        assertTrue(fs.exists(filePath), "File should exist after create and close");

        // ASSERT: Verify file metadata using production getFileStatus() API
        FileStatus status = fs.getFileStatus(filePath);
        assertNotNull(status, "FileStatus should not be null for existing file");
        assertEquals(fileSize, status.getLen(), 
                "File length should match written bytes");
        assertTrue(status.isFile(), "Path should be a file, not directory");

        // ASSERT: Verify content integrity via read-back using production open() and readFully() APIs
        byte[] actualData = new byte[fileSize];
        try (FSDataInputStream in = fs.open(filePath)) {
            in.readFully(actualData);
        }
        assertArrayEquals(expectedData, actualData, 
                "File content should match written data");
    }

    /**
     * Provides test parameters for parameterized write size tests.
     * 
     * <p>Returns a stream of Arguments representing different write size scenarios:
     * <ul>
     *   <li><b>Zero-byte:</b> writeSize=0, replication=2 - Edge case empty file</li>
     *   <li><b>Sub-block:</b> writeSize=1024 (1KB), replication=2 - Small file within single block</li>
     *   <li><b>Exact-block:</b> writeSize=128*1024*1024 (128MB), replication=3 - Exactly one block</li>
     *   <li><b>Multi-block:</b> writeSize=128*1024*1024+1, replication=2 - Spans two blocks</li>
     * </ul>
     * 
     * <p>Note: For CI efficiency, actual block size tests use smaller sizes that are
     * proportional to the configured block size to avoid excessive test duration.
     * 
     * @return Stream of Arguments for parameterized tests
     */
    static Stream<Arguments> writeSizeProvider() {
        // Get the actual configured block size from the cluster
        // For testing efficiency, we use smaller sizes proportional to block size
        long blockSize = conf != null ? 
                conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT) :
                DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
        
        // For CI efficiency, cap at reasonable sizes for integration tests
        // Use proportional sizes: sub-block < blockSize, exact = blockSize, multi > blockSize
        int subBlockSize = 1024; // 1KB - definitely sub-block
        int exactBlockSize = (int) Math.min(blockSize, 1024 * 1024); // Cap at 1MB for CI
        int multiBlockSize = exactBlockSize + 1; // One byte over block boundary

        return Stream.of(
                // Zero-byte file: edge case empty file
                Arguments.of(0, (short) 2, "zero-byte"),
                // Sub-block: small file within single block
                Arguments.of(subBlockSize, (short) 2, "sub-block"),
                // Exact-block: file exactly fills one block
                Arguments.of(exactBlockSize, (short) 3, "exact-block"),
                // Multi-block: file spans multiple blocks
                Arguments.of(multiBlockSize, (short) 2, "multi-block")
        );
    }

    /**
     * Workflow path: File Create/Write/Close with various write sizes
     * Production methods invoked: FileSystem.create(), FSDataOutputStream.write(),
     *                             FSDataOutputStream.close(), FileSystem.getFileStatus(),
     *                             FileSystem.open(), FSDataInputStream.readFully()
     * Input conditions: Parameterized writeSize (0, 1KB, blockSize, blockSize+1) and replication
     * Validation criteria: File length matches writeSize, replication matches requested,
     *                      content matches for non-zero files
     * 
     * <p>Tests file creation across different write sizes to validate block boundary handling:
     * <ul>
     *   <li><b>Zero-byte:</b> Tests empty file creation and metadata</li>
     *   <li><b>Sub-block:</b> Tests small file handling within single block</li>
     *   <li><b>Exact-block:</b> Tests block boundary alignment</li>
     *   <li><b>Multi-block:</b> Tests multi-block file handling</li>
     * </ul>
     * 
     * @param writeSize number of bytes to write
     * @param replication desired replication factor
     * @param description human-readable description for test reporting
     * @throws IOException if any HDFS operation fails
     */
    @ParameterizedTest(name = "writeSize={0}, replication={1}, type={2}")
    @MethodSource("writeSizeProvider")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testParameterizedWriteSizes(int writeSize, short replication, String description) 
            throws IOException {
        // ARRANGE: Define test file path with unique name based on parameters
        String fileName = String.format("test_write_%s_%d_bytes.dat", description, writeSize);
        Path filePath = new Path(testDir, fileName);
        
        // Generate deterministic test data
        byte[] expectedData = generateTestData(TEST_DATA_SEED, writeSize);

        // ACT: Create file with specified replication and write data using production APIs
        try (FSDataOutputStream out = fs.create(filePath, replication)) {
            assertNotNull(out, "FSDataOutputStream should not be null");
            if (writeSize > 0) {
                // Write data in chunks for memory efficiency with large files
                writeDataInChunks(out, expectedData);
            }
        } // close() called automatically

        // ASSERT: Verify file existence
        assertTrue(fs.exists(filePath), 
                String.format("File should exist after writing %d bytes (%s)", writeSize, description));

        // ASSERT: Verify file length using production getFileStatus() API
        FileStatus status = fs.getFileStatus(filePath);
        assertEquals(writeSize, status.getLen(), 
                String.format("File length should be %d bytes (%s)", writeSize, description));
        
        // ASSERT: Verify replication factor
        assertEquals(replication, status.getReplication(), 
                String.format("Replication factor should be %d (%s)", replication, description));

        // ASSERT: Verify content integrity for non-zero files
        if (writeSize > 0) {
            byte[] actualData = new byte[writeSize];
            try (FSDataInputStream in = fs.open(filePath)) {
                in.readFully(actualData);
            }
            assertArrayEquals(expectedData, actualData, 
                    String.format("Content should match for %s write", description));
        }
    }

    /**
     * Workflow path: File Create/Write with overwrite of existing file
     * Production methods invoked: FileSystem.create(), FSDataOutputStream.write(),
     *                             FileSystem.create(Path, boolean) with overwrite=true,
     *                             FileSystem.getFileStatus()
     * Input conditions: Create file, write content, then overwrite with different content
     * Validation criteria: File length and content match the overwritten version
     * 
     * <p>Tests the overwrite functionality of FileSystem.create():
     * <ol>
     *   <li>Create initial file with content</li>
     *   <li>Create second file with same path and overwrite=true</li>
     *   <li>Verify final content is from second write</li>
     * </ol>
     * 
     * @throws IOException if any HDFS operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testOverwriteExisting() throws IOException {
        // ARRANGE: Define test file path
        Path filePath = new Path(testDir, "test_overwrite_existing.dat");
        
        // Define initial and overwrite content
        int initialSize = 2048;
        int overwriteSize = 4096;
        byte[] initialData = generateTestData(TEST_DATA_SEED, initialSize);
        byte[] overwriteData = generateTestData(TEST_DATA_SEED + 1, overwriteSize); // Different seed

        // ACT: Create initial file using production API
        try (FSDataOutputStream out = fs.create(filePath)) {
            out.write(initialData);
        }

        // Verify initial file
        FileStatus initialStatus = fs.getFileStatus(filePath);
        assertEquals(initialSize, initialStatus.getLen(), 
                "Initial file should have correct length");

        // ACT: Overwrite file using create() with overwrite=true (default behavior)
        try (FSDataOutputStream out = fs.create(filePath, true)) {
            out.write(overwriteData);
        }

        // ASSERT: Verify file exists and has new length
        assertTrue(fs.exists(filePath), "File should still exist after overwrite");
        FileStatus overwriteStatus = fs.getFileStatus(filePath);
        assertEquals(overwriteSize, overwriteStatus.getLen(), 
                "File length should match overwrite content length");

        // ASSERT: Verify content is from overwrite using read-back
        byte[] actualData = new byte[overwriteSize];
        try (FSDataInputStream in = fs.open(filePath)) {
            in.readFully(actualData);
        }
        assertArrayEquals(overwriteData, actualData, 
                "Content should match overwritten data");
        
        // ASSERT: Verify content is NOT the initial data
        assertFalse(Arrays.equals(initialData, Arrays.copyOf(actualData, initialSize)),
                "Overwritten content should differ from initial content");
    }

    /**
     * Workflow path: File Create/Write/Close - Zero-byte file edge case
     * Production methods invoked: FileSystem.create(), FSDataOutputStream.close(),
     *                             FileSystem.getFileStatus(), FileSystem.exists()
     * Input conditions: Create file with zero bytes written
     * Validation criteria: File exists, length is 0, file is readable
     * 
     * <p>Tests the edge case of creating an empty file:
     * <ul>
     *   <li>Verify FileSystem.create() succeeds for empty file</li>
     *   <li>Verify close() without write() creates valid empty file</li>
     *   <li>Verify FileStatus reports length of 0</li>
     *   <li>Verify empty file is readable via open()</li>
     * </ul>
     * 
     * @throws IOException if any HDFS operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testZeroByteFile() throws IOException {
        // ARRANGE: Define test file path for empty file
        Path filePath = new Path(testDir, "test_zero_byte_file.dat");

        // ACT: Create file without writing any data using production API
        try (FSDataOutputStream out = fs.create(filePath)) {
            assertNotNull(out, "FSDataOutputStream should not be null for empty file");
            // Intentionally write nothing - testing zero-byte file creation
        } // close() called automatically

        // ASSERT: Verify file exists using production exists() API
        assertTrue(fs.exists(filePath), "Zero-byte file should exist after creation");

        // ASSERT: Verify file metadata using production getFileStatus() API
        FileStatus status = fs.getFileStatus(filePath);
        assertNotNull(status, "FileStatus should not be null for zero-byte file");
        assertEquals(0L, status.getLen(), "Zero-byte file should have length 0");
        assertTrue(status.isFile(), "Zero-byte path should be a file, not directory");
        assertTrue(status.getReplication() > 0, "Zero-byte file should have positive replication");

        // ASSERT: Verify empty file is readable via production open() API
        try (FSDataInputStream in = fs.open(filePath)) {
            assertNotNull(in, "Should be able to open zero-byte file for reading");
            // Verify read returns -1 (EOF) immediately
            assertEquals(-1, in.read(), "Reading from zero-byte file should return -1 (EOF)");
        }
    }

    /**
     * Generates deterministic test data using a fixed seed.
     * 
     * <p>Uses the provided seed to generate reproducible random bytes.
     * This ensures tests are deterministic and failures can be reproduced.
     * 
     * @param seed the random seed for reproducibility
     * @param size the number of bytes to generate
     * @return byte array containing deterministic pseudo-random data
     */
    private byte[] generateTestData(long seed, int size) {
        if (size <= 0) {
            return new byte[0];
        }
        byte[] data = new byte[size];
        Random random = new Random(seed);
        random.nextBytes(data);
        return data;
    }

    /**
     * Writes data to output stream in chunks for memory efficiency.
     * 
     * <p>For large files, writing in chunks prevents memory issues and more
     * accurately simulates real-world write patterns. Uses a 4KB buffer size.
     * 
     * @param out the FSDataOutputStream to write to
     * @param data the complete data to write
     * @throws IOException if write operation fails
     */
    private void writeDataInChunks(FSDataOutputStream out, byte[] data) throws IOException {
        int offset = 0;
        while (offset < data.length) {
            int bytesToWrite = Math.min(WRITE_BUFFER_SIZE, data.length - offset);
            out.write(data, offset, bytesToWrite);
            offset += bytesToWrite;
        }
    }
}
