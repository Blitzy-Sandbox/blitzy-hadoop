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

import java.io.EOFException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class for HDFS Workflow 2 (File Read Operations).
 * 
 * <p>This test class covers the complete file read workflow including:
 * <ul>
 *   <li>Full file read and content verification</li>
 *   <li>Seek operations at various positions (start, mid-block, block boundary, end)</li>
 *   <li>Positional read operations using read(position, buffer) and readFully()</li>
 *   <li>Edge cases like reading beyond EOF</li>
 * </ul>
 * 
 * <p>All tests invoke production APIs directly:
 * <ul>
 *   <li>{@code FileSystem.open()} - Opens files for reading</li>
 *   <li>{@code FSDataInputStream.read()} - Sequential and positional reads</li>
 *   <li>{@code FSDataInputStream.seek()} - Seeking to arbitrary positions</li>
 *   <li>{@code FSDataInputStream.readFully()} - Complete buffer reads</li>
 * </ul>
 * 
 * <h3>Test Data Generation</h3>
 * <p>Test files are created with deterministic content using a fixed random seed (0xDEADBEEF)
 * to ensure reproducible verification. The expected content is regenerated using the same
 * seed and compared byte-by-byte with the read content.
 * 
 * <h3>Block Boundary Testing</h3>
 * <p>The parameterized seek tests exercise reads at various positions relative to block
 * boundaries: position 0 (start), mid-block positions, exact block boundaries, and
 * positions near the end of file.
 * 
 * @see AbstractHdfsWorkflowTest
 * @see FSDataInputStream
 */
public class TestFileReadWorkflow extends AbstractHdfsWorkflowTest {

    /**
     * Random seed for deterministic test data generation.
     * Using the same seed for writing and verification ensures reproducible content.
     */
    private static final long SEED = 0xDEADBEEFL;

    /**
     * Default block size for test file creation.
     * Tests create files spanning multiple blocks to exercise cross-block reads.
     */
    private static final int BLOCK_SIZE = 4096;

    /**
     * Number of blocks in test files for multi-block read tests.
     * 12 blocks provides sufficient size for testing various seek positions.
     */
    private static final int NUM_BLOCKS = 12;

    /**
     * Total file size for multi-block tests.
     */
    private static final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE;

    /**
     * Replication factor for test files.
     * Using 3 replicas ensures production-like behavior.
     */
    private static final short REPLICATION = 3;

    /**
     * Workflow path: File Open and Full Read with content verification
     * Production methods invoked: FileSystem.create(), FSDataOutputStream.write(),
     *                             FileSystem.open(), FSDataInputStream.read()
     * Input conditions: File with deterministic random content (seed=0xDEADBEEF),
     *                   size=48KB (12 blocks * 4KB)
     * Validation criteria: All bytes read match expected content byte-by-byte
     * 
     * <p>This test validates the complete happy path for file reading:
     * <ol>
     *   <li>Create a test file with known content using deterministic random</li>
     *   <li>Open the file for reading using FileSystem.open()</li>
     *   <li>Read the entire file content into a buffer</li>
     *   <li>Verify the read content matches the expected content byte-by-byte</li>
     * </ol>
     *
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testFullRead() throws IOException {
        // ARRANGE: Create test file with deterministic content
        Path filePath = new Path(testDir, "fullread.dat");
        byte[] expectedContent = createTestFile(filePath, FILE_SIZE);

        // ACT: Open file and read all content using production APIs
        byte[] actualContent = new byte[FILE_SIZE];
        try (FSDataInputStream in = fs.open(filePath)) {
            int totalRead = 0;
            int bytesRead;
            // Read in a loop to handle partial reads
            while (totalRead < FILE_SIZE && 
                   (bytesRead = in.read(actualContent, totalRead, FILE_SIZE - totalRead)) > 0) {
                totalRead += bytesRead;
            }
            
            // ASSERT: Verify complete read
            assertEquals(FILE_SIZE, totalRead, "Should read entire file content");
        }

        // ASSERT: Verify content matches byte-by-byte
        assertArrayEquals(expectedContent, actualContent,
                "Read content should match written content exactly");

        // ASSERT: Verify file exists and has correct length
        assertTrue(fs.exists(filePath), "File should exist after read");
        assertEquals(FILE_SIZE, fs.getFileStatus(filePath).getLen(),
                "File length should match expected size");
    }

    /**
     * Provides test parameters for seek position testing.
     * 
     * <p>Returns seek positions covering various scenarios:
     * <ul>
     *   <li>Position 0: Start of file</li>
     *   <li>Mid-block position: BLOCK_SIZE / 2 (2048 bytes into first block)</li>
     *   <li>Block boundary: Exactly at BLOCK_SIZE (start of second block)</li>
     *   <li>Cross-block position: BLOCK_SIZE + 1024 (1KB into second block)</li>
     *   <li>Late file position: FILE_SIZE - BLOCK_SIZE (last block)</li>
     *   <li>Near end: FILE_SIZE - 100 (close to EOF)</li>
     * </ul>
     *
     * @return Stream of Arguments containing (seekPosition, readLength) tuples
     */
    static Stream<Arguments> seekPositionProvider() {
        return Stream.of(
            // Position at start of file
            Arguments.of(0L, 1024, "Start of file"),
            // Mid-block position (middle of first block)
            Arguments.of((long)(BLOCK_SIZE / 2), 2048, "Mid-block position"),
            // Exact block boundary (start of second block)
            Arguments.of((long)BLOCK_SIZE, 1024, "Block boundary"),
            // Position in second block
            Arguments.of((long)(BLOCK_SIZE + 1024), 2048, "Second block"),
            // Cross multiple blocks (from first to third block)
            Arguments.of((long)(BLOCK_SIZE - 512), BLOCK_SIZE + 1024, "Cross-block read"),
            // Late in file (last block)
            Arguments.of((long)(FILE_SIZE - BLOCK_SIZE), BLOCK_SIZE / 2, "Last block"),
            // Near end of file
            Arguments.of((long)(FILE_SIZE - 100), 100, "Near EOF")
        );
    }

    /**
     * Workflow path: Seek to various positions and read
     * Production methods invoked: FileSystem.open(), FSDataInputStream.seek(),
     *                             FSDataInputStream.read(), FSDataInputStream.getPos()
     * Input conditions: Different seek positions (0, mid-block, block boundary, near-EOF)
     * Validation criteria: Content read after seek matches expected bytes at that position
     * 
     * <p>This parameterized test validates seek and read operations at various positions:
     * <ol>
     *   <li>Create a test file with known content</li>
     *   <li>Open the file and seek to the specified position</li>
     *   <li>Verify the current position after seek</li>
     *   <li>Read specified number of bytes from that position</li>
     *   <li>Verify the read content matches expected content at that position</li>
     * </ol>
     *
     * @param seekPosition the position to seek to in the file
     * @param readLength the number of bytes to read after seeking
     * @param description human-readable description of the test case
     * @throws IOException if file operations fail
     */
    @ParameterizedTest(name = "Seek to {0}, read {1} bytes: {2}")
    @MethodSource("seekPositionProvider")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testParameterizedSeekRead(long seekPosition, int readLength, String description) 
            throws IOException {
        // ARRANGE: Create test file with deterministic content
        Path filePath = new Path(testDir, "seekread_" + seekPosition + ".dat");
        byte[] fullContent = createTestFile(filePath, FILE_SIZE);

        // ACT: Open file, seek to position, and read
        try (FSDataInputStream in = fs.open(filePath)) {
            // Perform seek operation
            in.seek(seekPosition);
            
            // ASSERT: Verify position after seek
            assertEquals(seekPosition, in.getPos(),
                    "Position should match seek target after seek()");

            // Read content from the seeked position
            byte[] readBuffer = new byte[readLength];
            int totalRead = 0;
            while (totalRead < readLength) {
                int bytesRead = in.read(readBuffer, totalRead, readLength - totalRead);
                if (bytesRead < 0) {
                    break; // EOF reached
                }
                totalRead += bytesRead;
            }

            // Extract expected content from full content array
            int expectedReadLength = Math.min(readLength, FILE_SIZE - (int) seekPosition);
            byte[] expectedContent = new byte[expectedReadLength];
            System.arraycopy(fullContent, (int) seekPosition, expectedContent, 0, expectedReadLength);

            // Truncate read buffer to actual bytes read if needed
            if (totalRead < readLength) {
                byte[] actualContent = new byte[totalRead];
                System.arraycopy(readBuffer, 0, actualContent, 0, totalRead);
                readBuffer = actualContent;
            }

            // ASSERT: Verify read content matches expected
            assertEquals(expectedReadLength, totalRead,
                    "Should read expected number of bytes for: " + description);
            
            assertArrayEquals(expectedContent, readBuffer,
                    "Content after seek should match expected for: " + description);
        }
    }

    /**
     * Workflow path: Positional read using readFully() and read(position, buffer)
     * Production methods invoked: FileSystem.open(), FSDataInputStream.readFully(),
     *                             FSDataInputStream.read(long, byte[], int, int)
     * Input conditions: File with multi-block content, various read positions
     * Validation criteria: Positional reads return correct content without affecting
     *                      stream position; readFully() reads complete buffer
     * 
     * <p>This test validates positional read APIs:
     * <ol>
     *   <li>Test readFully(buffer) - reads complete buffer from current position</li>
     *   <li>Test readFully(position, buffer) - reads complete buffer from specified position</li>
     *   <li>Test read(position, buffer, offset, length) - positional read without seek</li>
     *   <li>Verify that positional reads don't change the stream position</li>
     * </ol>
     *
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testPositionalRead() throws IOException {
        // ARRANGE: Create test file with deterministic content
        Path filePath = new Path(testDir, "positionalread.dat");
        byte[] fullContent = createTestFile(filePath, FILE_SIZE);

        try (FSDataInputStream in = fs.open(filePath)) {
            // Test 1: readFully() from current position (start)
            byte[] buffer1 = new byte[BLOCK_SIZE];
            in.readFully(buffer1);
            
            byte[] expected1 = new byte[BLOCK_SIZE];
            System.arraycopy(fullContent, 0, expected1, 0, BLOCK_SIZE);
            assertArrayEquals(expected1, buffer1,
                    "readFully() should read first block correctly");
            
            // Verify stream position moved forward
            assertEquals(BLOCK_SIZE, in.getPos(),
                    "Stream position should advance after readFully()");

            // Test 2: Positional readFully(position, buffer) - should NOT change stream position
            long savedPosition = in.getPos();
            byte[] buffer2 = new byte[2048];
            // Read from position 5000 (middle of second block)
            in.readFully(5000, buffer2);
            
            byte[] expected2 = new byte[2048];
            System.arraycopy(fullContent, 5000, expected2, 0, 2048);
            assertArrayEquals(expected2, buffer2,
                    "Positional readFully() should read correct content from position 5000");
            
            // Stream position should remain unchanged after positional read
            assertEquals(savedPosition, in.getPos(),
                    "Positional readFully() should not change stream position");

            // Test 3: read(position, buffer, offset, length) - positional read
            byte[] buffer3 = new byte[4096];
            // Read 2048 bytes from position 10000 into buffer at offset 1024
            int bytesRead = in.read(10000, buffer3, 1024, 2048);
            
            assertTrue(bytesRead > 0, "Positional read should return bytes read");
            
            byte[] expectedSlice = new byte[bytesRead];
            System.arraycopy(fullContent, 10000, expectedSlice, 0, bytesRead);
            
            byte[] actualSlice = new byte[bytesRead];
            System.arraycopy(buffer3, 1024, actualSlice, 0, bytesRead);
            
            assertArrayEquals(expectedSlice, actualSlice,
                    "Positional read(position, buffer, offset, length) should read correct content");

            // Stream position should still be unchanged
            assertEquals(savedPosition, in.getPos(),
                    "Positional read should not change stream position");

            // Test 4: readFully(position, buffer, offset, length)
            byte[] buffer4 = new byte[3000];
            // Read 1500 bytes from position 20000 into buffer at offset 500
            in.readFully(20000, buffer4, 500, 1500);
            
            byte[] expected4 = new byte[1500];
            System.arraycopy(fullContent, 20000, expected4, 0, 1500);
            
            byte[] actual4 = new byte[1500];
            System.arraycopy(buffer4, 500, actual4, 0, 1500);
            
            assertArrayEquals(expected4, actual4,
                    "Positional readFully(position, buffer, offset, length) should read correct content");

            // Test 5: Cross-block positional read
            // Read across block boundary starting near end of first block
            byte[] buffer5 = new byte[BLOCK_SIZE + 1024];
            in.readFully(BLOCK_SIZE - 512, buffer5);
            
            byte[] expected5 = new byte[BLOCK_SIZE + 1024];
            System.arraycopy(fullContent, BLOCK_SIZE - 512, expected5, 0, BLOCK_SIZE + 1024);
            assertArrayEquals(expected5, buffer5,
                    "Cross-block positional read should read correct content");
        }
    }

    /**
     * Workflow path: Read beyond end of file (EOF edge case)
     * Production methods invoked: FileSystem.open(), FSDataInputStream.readFully(),
     *                             FSDataInputStream.read()
     * Input conditions: Attempt to read more bytes than available, read from EOF position
     * Validation criteria: readFully() throws EOFException; read() returns -1 at EOF
     * 
     * <p>This test validates proper handling of read operations at and beyond EOF:
     * <ol>
     *   <li>Test read() returning -1 when at EOF</li>
     *   <li>Test readFully() throwing EOFException when buffer cannot be filled</li>
     *   <li>Test positional read beyond file length</li>
     * </ol>
     *
     * @throws IOException if file operations fail unexpectedly
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testReadBeyondEOF() throws IOException {
        // ARRANGE: Create a small test file
        Path filePath = new Path(testDir, "eoftest.dat");
        int fileSize = 1000; // Small file for EOF testing
        createTestFile(filePath, fileSize);

        try (FSDataInputStream in = fs.open(filePath)) {
            // Test 1: Sequential read to EOF should return -1
            byte[] buffer = new byte[fileSize + 500];
            int totalRead = 0;
            int bytesRead;
            while ((bytesRead = in.read(buffer, totalRead, buffer.length - totalRead)) > 0) {
                totalRead += bytesRead;
            }
            
            // Verify we read exactly the file size
            assertEquals(fileSize, totalRead, "Should read exactly file size bytes");
            
            // Next read should return -1 (EOF)
            assertEquals(-1, in.read(), "Read at EOF should return -1");
            assertEquals(-1, in.read(new byte[10], 0, 10), "Read into buffer at EOF should return -1");

            // Test 2: Seek to end and try to read
            in.seek(fileSize);
            assertEquals(fileSize, in.getPos(), "Position should be at EOF");
            assertEquals(-1, in.read(), "Read after seeking to EOF should return -1");

            // Test 3: readFully() when not enough bytes available should throw EOFException
            in.seek(fileSize - 100); // 100 bytes from end
            byte[] tooLargeBuffer = new byte[200]; // Request more than available
            
            assertThrows(EOFException.class, () -> {
                in.readFully(tooLargeBuffer);
            }, "readFully() should throw EOFException when buffer cannot be filled");

            // Test 4: Positional readFully() beyond EOF should throw EOFException
            assertThrows(EOFException.class, () -> {
                byte[] beyondEof = new byte[100];
                in.readFully(fileSize - 50, beyondEof); // Request 100 bytes when only 50 available
            }, "Positional readFully() beyond EOF should throw EOFException");

            // Test 5: readFully() from exactly EOF position with non-zero length
            assertThrows(EOFException.class, () -> {
                byte[] atEof = new byte[1];
                in.readFully(fileSize, atEof); // Read from EOF position
            }, "readFully() at EOF position should throw EOFException");

            // Test 6: readFully() with zero-length buffer at EOF should succeed
            byte[] emptyBuffer = new byte[0];
            in.readFully(fileSize, emptyBuffer); // Should not throw
            
            // Test 7: Positional read() beyond EOF should return -1
            byte[] buffer2 = new byte[100];
            int result = in.read(fileSize + 1000, buffer2, 0, 100);
            assertEquals(-1, result, "Positional read beyond file should return -1");
        }
    }

    /**
     * Creates a test file with deterministic random content.
     * 
     * <p>The file content is generated using a Random initialized with SEED,
     * ensuring that the same seed can be used to regenerate expected content
     * for verification.
     *
     * @param path the HDFS path where the file should be created
     * @param size the size of the file in bytes
     * @return the byte array containing the file's expected content
     * @throws IOException if file creation fails
     */
    private byte[] createTestFile(Path path, int size) throws IOException {
        // Generate deterministic content using fixed seed
        byte[] content = new byte[size];
        Random random = new Random(SEED);
        random.nextBytes(content);

        // Write content to HDFS using production APIs
        try (FSDataOutputStream out = fs.create(path, REPLICATION)) {
            out.write(content);
        }

        // Verify file was created with correct size
        assertTrue(fs.exists(path), "Test file should exist after creation");
        assertEquals(size, fs.getFileStatus(path).getLen(),
                "Test file should have correct length");

        return content;
    }
}
