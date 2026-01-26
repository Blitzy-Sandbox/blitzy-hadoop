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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class covering HDFS Workflow 3: File Delete operations.
 * 
 * <p>This test class validates the complete file deletion workflow using
 * production APIs {@code FileSystem.delete()} and {@code FileSystem.exists()}.
 * It covers various deletion scenarios including:
 * <ul>
 *   <li>Single file deletion with verification</li>
 *   <li>Recursive directory deletion with nested content</li>
 *   <li>Non-existent file deletion behavior</li>
 *   <li>Non-recursive deletion of non-empty directories</li>
 * </ul>
 * 
 * <h3>Production Methods Tested</h3>
 * <ul>
 *   <li>{@code FileSystem.delete(Path, boolean)} - Delete files/directories with recursive option</li>
 *   <li>{@code FileSystem.exists(Path)} - Verify existence after deletion</li>
 *   <li>{@code FileSystem.create(Path)} - Create test files for deletion</li>
 *   <li>{@code FileSystem.mkdirs(Path)} - Create test directories for deletion</li>
 * </ul>
 * 
 * <h3>Test Isolation</h3>
 * <p>All tests use the unique {@code testDir} provided by {@link AbstractHdfsWorkflowTest}
 * to ensure complete isolation between test methods. Each test creates its own
 * files and directories within this namespace.
 * 
 * <h3>Compliance</h3>
 * <ul>
 *   <li>All test methods use {@code @Timeout(60)} for 60-second execution limit</li>
 *   <li>No {@code Thread.sleep()} calls - async operations use GenericTestUtils.waitFor() if needed</li>
 *   <li>All assertions validate production API return values and side effects</li>
 *   <li>Zero reimplemented business logic - all operations use production code</li>
 * </ul>
 * 
 * @see AbstractHdfsWorkflowTest
 * @see org.apache.hadoop.fs.FileSystem#delete(Path, boolean)
 * @see org.apache.hadoop.fs.FileSystem#exists(Path)
 */
public class TestFileDeleteWorkflow extends AbstractHdfsWorkflowTest {

    /**
     * Test content used for creating test files.
     * Using a deterministic string for consistent testing.
     */
    private static final byte[] TEST_CONTENT = "Test file content for deletion workflow".getBytes(StandardCharsets.UTF_8);

    /**
     * Workflow: Delete a single file and verify deletion.
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.create(Path)} - Create test file</li>
     *   <li>{@code FSDataOutputStream.write(byte[])} - Write content</li>
     *   <li>{@code FSDataOutputStream.close()} - Close stream</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify file exists before deletion</li>
     *   <li>{@code FileSystem.delete(Path, false)} - Delete file (non-recursive)</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify file is deleted</li>
     * </ul>
     * 
     * <p>Input conditions: Single file with content in test directory.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>File exists after creation</li>
     *   <li>delete() returns true for existing file</li>
     *   <li>exists() returns false after deletion</li>
     * </ul>
     * 
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Delete single file and verify exists() returns false")
    void testDeleteSingleFile() throws IOException {
        // ARRANGE: Create a test file with content
        Path filePath = new Path(testDir, "single-file-to-delete.txt");
        
        // Create file using production API
        try (FSDataOutputStream out = fs.create(filePath)) {
            out.write(TEST_CONTENT);
        }
        
        // Verify file exists using production API
        assertTrue(fs.exists(filePath), 
            "File should exist after creation");
        
        // Verify we can get file status (additional existence check)
        FileStatus status = fs.getFileStatus(filePath);
        assertNotNull(status, "FileStatus should not be null for existing file");
        assertTrue(status.isFile(), "Path should be a file");
        
        // ACT: Delete the file using production API (non-recursive)
        boolean deleteResult = fs.delete(filePath, false);
        
        // ASSERT: Verify deletion was successful
        assertTrue(deleteResult, 
            "delete() should return true for existing file");
        assertFalse(fs.exists(filePath), 
            "exists() should return false after file deletion");
    }

    /**
     * Workflow: Create a directory tree and delete it recursively.
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create directory structure</li>
     *   <li>{@code FileSystem.create(Path)} - Create files in directories</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify structure exists</li>
     *   <li>{@code FileSystem.delete(Path, true)} - Delete directory recursively</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify all content is deleted</li>
     * </ul>
     * 
     * <p>Input conditions: Directory tree with nested subdirectories and files:
     * <pre>
     * testDir/
     *   recursive-dir/
     *     file1.txt
     *     subdir1/
     *       file2.txt
     *       subdir2/
     *         file3.txt
     * </pre>
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>All directories and files exist after creation</li>
     *   <li>delete() with recursive=true returns true</li>
     *   <li>All directories and files are removed after deletion</li>
     * </ul>
     * 
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Delete directory tree recursively and verify all content is removed")
    void testDeleteRecursive() throws IOException {
        // ARRANGE: Create a directory tree with multiple levels
        Path rootDir = new Path(testDir, "recursive-dir");
        Path subDir1 = new Path(rootDir, "subdir1");
        Path subDir2 = new Path(subDir1, "subdir2");
        
        Path file1 = new Path(rootDir, "file1.txt");
        Path file2 = new Path(subDir1, "file2.txt");
        Path file3 = new Path(subDir2, "file3.txt");
        
        // Create the deepest directory (creates all parents)
        assertTrue(fs.mkdirs(subDir2), 
            "mkdirs() should return true for directory creation");
        
        // Create files at various levels using production APIs
        try (FSDataOutputStream out1 = fs.create(file1)) {
            out1.write(TEST_CONTENT);
        }
        try (FSDataOutputStream out2 = fs.create(file2)) {
            out2.write(TEST_CONTENT);
        }
        try (FSDataOutputStream out3 = fs.create(file3)) {
            out3.write(TEST_CONTENT);
        }
        
        // Verify structure exists using production APIs
        assertTrue(fs.exists(rootDir), "Root directory should exist");
        assertTrue(fs.exists(subDir1), "SubDir1 should exist");
        assertTrue(fs.exists(subDir2), "SubDir2 should exist");
        assertTrue(fs.exists(file1), "File1 should exist");
        assertTrue(fs.exists(file2), "File2 should exist");
        assertTrue(fs.exists(file3), "File3 should exist");
        
        // ACT: Delete the root directory recursively
        boolean deleteResult = fs.delete(rootDir, true);
        
        // ASSERT: Verify all content is deleted
        assertTrue(deleteResult, 
            "delete() with recursive=true should return true");
        assertFalse(fs.exists(rootDir), 
            "Root directory should not exist after recursive deletion");
        assertFalse(fs.exists(subDir1), 
            "SubDir1 should not exist after recursive deletion");
        assertFalse(fs.exists(subDir2), 
            "SubDir2 should not exist after recursive deletion");
        assertFalse(fs.exists(file1), 
            "File1 should not exist after recursive deletion");
        assertFalse(fs.exists(file2), 
            "File2 should not exist after recursive deletion");
        assertFalse(fs.exists(file3), 
            "File3 should not exist after recursive deletion");
    }

    /**
     * Workflow: Attempt to delete a non-existent file.
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.exists(Path)} - Verify file does not exist</li>
     *   <li>{@code FileSystem.delete(Path, false)} - Attempt deletion</li>
     * </ul>
     * 
     * <p>Input conditions: Path that does not exist in HDFS.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>Path does not exist before deletion attempt</li>
     *   <li>delete() returns false for non-existent path (per HDFS semantics)</li>
     * </ul>
     * 
     * <p>Note: HDFS FileSystem.delete() returns false when the path does not exist,
     * rather than throwing an exception. This behavior is consistent with the
     * FileSystem contract.
     * 
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Delete non-existent file returns false")
    void testDeleteNonExistent() throws IOException {
        // ARRANGE: Define a path that does not exist
        Path nonExistentPath = new Path(testDir, "non-existent-file.txt");
        
        // Verify the path does not exist using production API
        assertFalse(fs.exists(nonExistentPath), 
            "Path should not exist before deletion attempt");
        
        // ACT: Attempt to delete the non-existent path
        boolean deleteResultNonRecursive = fs.delete(nonExistentPath, false);
        boolean deleteResultRecursive = fs.delete(nonExistentPath, true);
        
        // ASSERT: Both deletion attempts should return false
        assertFalse(deleteResultNonRecursive, 
            "delete(path, false) should return false for non-existent path");
        assertFalse(deleteResultRecursive, 
            "delete(path, true) should return false for non-existent path");
        
        // Path should still not exist (sanity check)
        assertFalse(fs.exists(nonExistentPath), 
            "Path should still not exist after deletion attempts");
    }

    /**
     * Workflow: Attempt non-recursive deletion of a non-empty directory.
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create directory</li>
     *   <li>{@code FileSystem.create(Path)} - Create file in directory</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify structure exists</li>
     *   <li>{@code FileSystem.delete(Path, false)} - Attempt non-recursive deletion</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify directory still exists</li>
     * </ul>
     * 
     * <p>Input conditions: Directory containing one or more files.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>Directory and file exist after creation</li>
     *   <li>Non-recursive delete() on non-empty directory throws PathIsNotEmptyDirectoryException</li>
     *   <li>Directory and contents remain intact after failed deletion attempt</li>
     * </ul>
     * 
     * <p>Note: HDFS throws {@link PathIsNotEmptyDirectoryException} when attempting 
     * non-recursive deletion of a non-empty directory. This test validates both the
     * exception being thrown and the preservation of directory contents.
     * 
     * @throws IOException if file operations fail unexpectedly
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Non-recursive delete on non-empty directory should fail")
    void testDeleteNonEmptyNonRecursive() throws IOException {
        // ARRANGE: Create a non-empty directory
        Path dirPath = new Path(testDir, "non-empty-dir");
        Path filePath = new Path(dirPath, "file-inside.txt");
        
        // Create directory using production API
        assertTrue(fs.mkdirs(dirPath), 
            "mkdirs() should return true for directory creation");
        
        // Create a file inside the directory
        try (FSDataOutputStream out = fs.create(filePath)) {
            out.write(TEST_CONTENT);
        }
        
        // Verify both directory and file exist
        assertTrue(fs.exists(dirPath), 
            "Directory should exist after creation");
        assertTrue(fs.exists(filePath), 
            "File should exist after creation");
        
        // Verify directory is indeed a directory and contains content
        FileStatus dirStatus = fs.getFileStatus(dirPath);
        assertTrue(dirStatus.isDirectory(), 
            "Path should be a directory");
        FileStatus[] contents = fs.listStatus(dirPath);
        assertTrue(contents.length > 0, 
            "Directory should have contents");
        
        // ACT & ASSERT: Attempt non-recursive deletion should throw exception
        // HDFS throws PathIsNotEmptyDirectoryException for non-recursive delete of non-empty directory
        IOException thrownException = assertThrows(IOException.class, () -> {
            fs.delete(dirPath, false);
        }, "delete(path, false) should throw IOException for non-empty directory");
        
        // Verify the exception is specifically PathIsNotEmptyDirectoryException
        assertInstanceOf(PathIsNotEmptyDirectoryException.class, thrownException,
            "Exception should be PathIsNotEmptyDirectoryException");
        
        // ASSERT: Directory and contents should remain intact after failed deletion
        assertTrue(fs.exists(dirPath), 
            "Directory should still exist after failed deletion attempt");
        assertTrue(fs.exists(filePath), 
            "File inside directory should still exist after failed deletion attempt");
        
        // Verify contents are unchanged
        FileStatus[] contentsAfter = fs.listStatus(dirPath);
        assertTrue(contentsAfter.length > 0, 
            "Directory should still have contents after failed deletion");
    }

    /**
     * Workflow: Delete an empty directory with non-recursive flag.
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create empty directory</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify directory exists</li>
     *   <li>{@code FileSystem.delete(Path, false)} - Delete empty directory</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify directory is deleted</li>
     * </ul>
     * 
     * <p>Input conditions: Empty directory in test namespace.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>Empty directory exists after creation</li>
     *   <li>delete() with recursive=false returns true for empty directory</li>
     *   <li>Directory does not exist after deletion</li>
     * </ul>
     * 
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Delete empty directory with non-recursive flag succeeds")
    void testDeleteEmptyDirectoryNonRecursive() throws IOException {
        // ARRANGE: Create an empty directory
        Path emptyDir = new Path(testDir, "empty-dir");
        
        assertTrue(fs.mkdirs(emptyDir), 
            "mkdirs() should return true for directory creation");
        assertTrue(fs.exists(emptyDir), 
            "Directory should exist after creation");
        
        // Verify directory is empty
        FileStatus dirStatus = fs.getFileStatus(emptyDir);
        assertTrue(dirStatus.isDirectory(), 
            "Path should be a directory");
        FileStatus[] contents = fs.listStatus(emptyDir);
        assertTrue(contents.length == 0, 
            "Directory should be empty");
        
        // ACT: Delete empty directory with non-recursive flag
        boolean deleteResult = fs.delete(emptyDir, false);
        
        // ASSERT: Deletion should succeed
        assertTrue(deleteResult, 
            "delete(path, false) should return true for empty directory");
        assertFalse(fs.exists(emptyDir), 
            "Empty directory should not exist after deletion");
    }

    /**
     * Workflow: Delete a file using recursive flag (should work same as non-recursive).
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.create(Path)} - Create test file</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify file exists</li>
     *   <li>{@code FileSystem.delete(Path, true)} - Delete file with recursive flag</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify file is deleted</li>
     * </ul>
     * 
     * <p>Input conditions: Single file in test directory.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>File exists after creation</li>
     *   <li>delete() with recursive=true returns true for single file</li>
     *   <li>File does not exist after deletion</li>
     * </ul>
     * 
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Delete single file with recursive flag succeeds")
    void testDeleteSingleFileRecursive() throws IOException {
        // ARRANGE: Create a test file
        Path filePath = new Path(testDir, "file-to-delete-recursive.txt");
        
        try (FSDataOutputStream out = fs.create(filePath)) {
            out.write(TEST_CONTENT);
        }
        
        assertTrue(fs.exists(filePath), 
            "File should exist after creation");
        
        // Verify it's a file
        FileStatus status = fs.getFileStatus(filePath);
        assertTrue(status.isFile(), 
            "Path should be a file");
        
        // ACT: Delete file with recursive flag
        boolean deleteResult = fs.delete(filePath, true);
        
        // ASSERT: Deletion should succeed
        assertTrue(deleteResult, 
            "delete(path, true) should return true for existing file");
        assertFalse(fs.exists(filePath), 
            "File should not exist after deletion");
    }

    /**
     * Workflow: Multiple deletions on the same path.
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.create(Path)} - Create test file</li>
     *   <li>{@code FileSystem.delete(Path, false)} - Delete file (first call)</li>
     *   <li>{@code FileSystem.delete(Path, false)} - Delete file (second call)</li>
     * </ul>
     * 
     * <p>Input conditions: File that is deleted and then deletion is attempted again.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>First delete() returns true (file existed)</li>
     *   <li>Second delete() returns false (file no longer exists)</li>
     * </ul>
     * 
     * @throws IOException if file operations fail
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Second deletion attempt on same path returns false")
    void testDoubleDelete() throws IOException {
        // ARRANGE: Create a test file
        Path filePath = new Path(testDir, "double-delete-file.txt");
        
        try (FSDataOutputStream out = fs.create(filePath)) {
            out.write(TEST_CONTENT);
        }
        
        assertTrue(fs.exists(filePath), 
            "File should exist after creation");
        
        // ACT: Delete the file twice
        boolean firstDeleteResult = fs.delete(filePath, false);
        boolean secondDeleteResult = fs.delete(filePath, false);
        
        // ASSERT: First deletion should succeed, second should return false
        assertTrue(firstDeleteResult, 
            "First delete() should return true for existing file");
        assertFalse(secondDeleteResult, 
            "Second delete() should return false (file already deleted)");
        assertFalse(fs.exists(filePath), 
            "File should not exist after deletion");
    }
}
