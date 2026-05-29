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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class covering HDFS Workflow 4: Directory Operations.
 * 
 * <p>This test class validates the core directory operations in HDFS:
 * <ul>
 *   <li>{@code FileSystem.mkdirs()} - Creating directories with nested paths</li>
 *   <li>{@code FileSystem.listStatus()} - Listing directory contents</li>
 *   <li>{@code FileSystem.rename()} - Renaming files and directories</li>
 * </ul>
 * 
 * <h3>Workflow Coverage</h3>
 * <p>This class tests Workflow 4 (Directory Operations) from the critical workflows:
 * <ul>
 *   <li>Happy path: Create directory structure, list contents, rename successfully</li>
 *   <li>Idempotent mkdirs: Calling mkdirs on existing directory returns true</li>
 *   <li>Rename edge cases: Rename to existing directory path behavior</li>
 *   <li>Empty directory listing: listStatus on empty directory returns empty array</li>
 * </ul>
 * 
 * <h3>Production APIs Exercised</h3>
 * <ul>
 *   <li>{@code FileSystem.mkdirs(Path)} - Create directories including parents</li>
 *   <li>{@code FileSystem.listStatus(Path)} - List file/directory status entries</li>
 *   <li>{@code FileSystem.rename(Path, Path)} - Rename/move files and directories</li>
 *   <li>{@code FileSystem.exists(Path)} - Check path existence</li>
 *   <li>{@code FileSystem.getFileStatus(Path)} - Get status of single path</li>
 *   <li>{@code FileSystem.isDirectory(Path)} - Check if path is directory</li>
 * </ul>
 * 
 * <h3>Test Isolation</h3>
 * <p>Each test method operates in an isolated directory under the /workflow namespace,
 * provided by {@link AbstractHdfsWorkflowTest}. The test directory is automatically
 * created before each test and deleted after each test.
 * 
 * <h3>Performance Requirements</h3>
 * <p>All tests are annotated with {@code @Timeout(60)} to enforce the 60-second
 * per-test execution limit required for the 45-minute CI budget.
 * 
 * @see AbstractHdfsWorkflowTest
 * @see FileSystem#mkdirs(Path)
 * @see FileSystem#listStatus(Path)
 * @see FileSystem#rename(Path, Path)
 */
public class TestDirectoryOperationsWorkflow extends AbstractHdfsWorkflowTest {

    /**
     * Tests the happy path workflow for directory operations.
     * 
     * <p>Workflow path: Create nested directories → Add files → List contents → Rename directory
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create nested directory structure</li>
     *   <li>{@code FileSystem.create(Path)} - Create files in directories</li>
     *   <li>{@code FileSystem.listStatus(Path)} - List directory contents</li>
     *   <li>{@code FileSystem.rename(Path, Path)} - Rename directory</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify path existence</li>
     *   <li>{@code FileSystem.isDirectory(Path)} - Verify directory type</li>
     * </ul>
     * 
     * <p>Input conditions: Create a nested directory structure /testDir/parent/child,
     * create files in the directories, list contents, then rename the parent directory.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>mkdirs returns true for new directory creation</li>
     *   <li>All created paths exist and are directories</li>
     *   <li>listStatus returns expected file/directory entries</li>
     *   <li>rename succeeds and old path no longer exists</li>
     *   <li>Renamed directory contains all original contents</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMkdirsListRename() throws IOException {
        // ARRANGE: Define directory structure
        Path parentDir = new Path(testDir, "parent");
        Path childDir = new Path(parentDir, "child");
        Path grandchildDir = new Path(childDir, "grandchild");

        // ACT: Create nested directory structure using production mkdirs
        boolean parentCreated = fs.mkdirs(parentDir);
        boolean childCreated = fs.mkdirs(childDir);
        boolean grandchildCreated = fs.mkdirs(grandchildDir);

        // ASSERT: Verify directory creation
        assertTrue(parentCreated, "mkdirs should return true for new parent directory");
        assertTrue(childCreated, "mkdirs should return true for new child directory");
        assertTrue(grandchildCreated, "mkdirs should return true for new grandchild directory");

        // Verify all directories exist using production exists()
        assertTrue(fs.exists(parentDir), "Parent directory should exist");
        assertTrue(fs.exists(childDir), "Child directory should exist");
        assertTrue(fs.exists(grandchildDir), "Grandchild directory should exist");

        // Verify all paths are directories using production isDirectory()
        assertTrue(fs.isDirectory(parentDir), "Parent should be a directory");
        assertTrue(fs.isDirectory(childDir), "Child should be a directory");
        assertTrue(fs.isDirectory(grandchildDir), "Grandchild should be a directory");

        // ACT: Create files in the directories using production create()
        Path fileInChild = new Path(childDir, "testfile.txt");
        byte[] testContent = "Hello from directory operations test".getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream out = fs.create(fileInChild)) {
            out.write(testContent);
        }

        // ACT: List parent directory contents using production listStatus()
        FileStatus[] parentContents = fs.listStatus(parentDir);

        // ASSERT: Parent directory contains one entry (child directory)
        assertNotNull(parentContents, "listStatus should not return null");
        assertEquals(1, parentContents.length, "Parent directory should contain one entry");
        assertEquals("child", parentContents[0].getPath().getName(), 
                "Parent should contain child directory");
        assertTrue(parentContents[0].isDirectory(), "Entry should be a directory");

        // ACT: List child directory contents
        FileStatus[] childContents = fs.listStatus(childDir);

        // ASSERT: Child directory contains two entries (grandchild dir and file)
        assertNotNull(childContents, "listStatus should not return null for child");
        assertEquals(2, childContents.length, 
                "Child directory should contain two entries (grandchild dir and testfile)");

        // Verify we have both expected entries (order may vary)
        boolean hasGrandchild = false;
        boolean hasFile = false;
        for (FileStatus status : childContents) {
            String name = status.getPath().getName();
            if ("grandchild".equals(name) && status.isDirectory()) {
                hasGrandchild = true;
            } else if ("testfile.txt".equals(name) && status.isFile()) {
                hasFile = true;
            }
        }
        assertTrue(hasGrandchild, "Child directory should contain grandchild subdirectory");
        assertTrue(hasFile, "Child directory should contain testfile.txt");

        // ACT: Rename parent directory using production rename()
        Path renamedParent = new Path(testDir, "renamed_parent");
        boolean renameSuccess = fs.rename(parentDir, renamedParent);

        // ASSERT: Rename operation results
        assertTrue(renameSuccess, "Rename should succeed for directory");
        assertFalse(fs.exists(parentDir), "Original parent path should not exist after rename");
        assertTrue(fs.exists(renamedParent), "Renamed parent path should exist");
        assertTrue(fs.isDirectory(renamedParent), "Renamed path should be a directory");

        // ASSERT: Renamed directory contains all original contents
        Path renamedChild = new Path(renamedParent, "child");
        Path renamedFile = new Path(renamedChild, "testfile.txt");
        Path renamedGrandchild = new Path(renamedChild, "grandchild");

        assertTrue(fs.exists(renamedChild), "Renamed child directory should exist");
        assertTrue(fs.exists(renamedFile), "File in renamed directory should exist");
        assertTrue(fs.exists(renamedGrandchild), "Grandchild in renamed directory should exist");

        // Verify file content is preserved after rename
        FileStatus fileStatus = fs.getFileStatus(renamedFile);
        assertEquals(testContent.length, fileStatus.getLen(), 
                "File size should be preserved after directory rename");
    }

    /**
     * Tests that mkdirs is idempotent - calling mkdirs on an existing directory
     * returns true without error.
     * 
     * <p>Workflow path: Create directory → Call mkdirs again on same path
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create directory and verify idempotency</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify directory existence</li>
     *   <li>{@code FileSystem.isDirectory(Path)} - Verify directory type</li>
     * </ul>
     * 
     * <p>Input conditions: Create a new directory, then call mkdirs again on the same path.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>First mkdirs returns true for new directory</li>
     *   <li>Second mkdirs returns true (idempotent behavior)</li>
     *   <li>Directory exists and is a directory after both calls</li>
     *   <li>No exception is thrown on the second call</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMkdirsIdempotent() throws IOException {
        // ARRANGE: Define directory path
        Path targetDir = new Path(testDir, "idempotent_dir");

        // ASSERT: Directory does not exist initially
        assertFalse(fs.exists(targetDir), "Directory should not exist initially");

        // ACT: Create directory first time using production mkdirs()
        boolean firstCreate = fs.mkdirs(targetDir);

        // ASSERT: First creation succeeds
        assertTrue(firstCreate, "First mkdirs should return true for new directory");
        assertTrue(fs.exists(targetDir), "Directory should exist after first mkdirs");
        assertTrue(fs.isDirectory(targetDir), "Path should be a directory after first mkdirs");

        // ACT: Call mkdirs again on existing directory (idempotent operation)
        boolean secondCreate = fs.mkdirs(targetDir);

        // ASSERT: Second creation also returns true (idempotent behavior)
        assertTrue(secondCreate, "Second mkdirs on existing directory should return true");
        assertTrue(fs.exists(targetDir), "Directory should still exist after second mkdirs");
        assertTrue(fs.isDirectory(targetDir), "Path should still be a directory after second mkdirs");

        // ACT: Test nested idempotency - create parent/child, then call mkdirs on parent again
        Path nestedChild = new Path(targetDir, "nested/deep/child");
        boolean nestedCreate = fs.mkdirs(nestedChild);

        assertTrue(nestedCreate, "Nested mkdirs should succeed");
        assertTrue(fs.exists(nestedChild), "Nested child should exist");

        // ACT: Call mkdirs on parent after child exists
        Path intermediateParent = new Path(targetDir, "nested");
        boolean parentIdempotent = fs.mkdirs(intermediateParent);

        // ASSERT: Parent mkdirs returns true even when child exists
        assertTrue(parentIdempotent, 
                "mkdirs on existing parent (with child) should return true");
        assertTrue(fs.exists(nestedChild), 
                "Nested child should still exist after mkdirs on parent");
    }

    /**
     * Tests rename behavior when the target path already exists.
     * 
     * <p>Workflow path: Create source directory → Create target directory → Attempt rename
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create source and target directories</li>
     *   <li>{@code FileSystem.rename(Path, Path)} - Attempt rename to existing path</li>
     *   <li>{@code FileSystem.rename(Path, Path, Options.Rename)} - Rename with OVERWRITE</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify path existence</li>
     *   <li>{@code FileSystem.listStatus(Path)} - Verify directory contents</li>
     * </ul>
     * 
     * <p>Input conditions: Create a source directory with content and an empty target directory,
     * then attempt rename operations.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>Rename to existing empty directory succeeds (moves source into target)</li>
     *   <li>Source directory no longer exists at original path</li>
     *   <li>Source directory exists as subdirectory of target</li>
     *   <li>Original target directory still exists</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRenameToExisting() throws IOException {
        // ARRANGE: Create source directory with a file
        Path sourceDir = new Path(testDir, "source_dir");
        Path targetDir = new Path(testDir, "target_dir");

        assertTrue(fs.mkdirs(sourceDir), "Source directory creation should succeed");
        assertTrue(fs.mkdirs(targetDir), "Target directory creation should succeed");

        // Create a file in source directory
        Path sourceFile = new Path(sourceDir, "source_file.txt");
        byte[] sourceContent = "Source file content".getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream out = fs.create(sourceFile)) {
            out.write(sourceContent);
        }

        // Verify initial state
        assertTrue(fs.exists(sourceDir), "Source directory should exist");
        assertTrue(fs.exists(targetDir), "Target directory should exist");
        assertTrue(fs.exists(sourceFile), "Source file should exist");

        // ACT: Rename source directory to existing target directory
        // In HDFS, renaming to an existing directory moves the source as a subdirectory
        boolean renameResult = fs.rename(sourceDir, targetDir);

        // ASSERT: Rename behavior - source moves into target as subdirectory
        // When target exists and is a directory, source becomes a child of target
        assertTrue(renameResult, "Rename to existing directory should succeed");
        assertFalse(fs.exists(sourceDir), 
                "Original source path should not exist after rename");
        assertTrue(fs.exists(targetDir), "Target directory should still exist");

        // The source directory should now exist as a subdirectory of target
        Path movedSource = new Path(targetDir, "source_dir");
        assertTrue(fs.exists(movedSource), 
                "Source should exist as subdirectory of target");
        assertTrue(fs.isDirectory(movedSource), "Moved source should be a directory");

        // Verify the file was moved along with the directory
        Path movedFile = new Path(movedSource, "source_file.txt");
        assertTrue(fs.exists(movedFile), "Source file should exist in moved location");

        // Verify target directory contents
        FileStatus[] targetContents = fs.listStatus(targetDir);
        assertNotNull(targetContents, "Target directory listing should not be null");
        assertEquals(1, targetContents.length, 
                "Target should contain one entry (the moved source directory)");
        assertEquals("source_dir", targetContents[0].getPath().getName(),
                "Target should contain the moved source directory");
    }

    /**
     * Tests that listStatus on an empty directory returns an empty array.
     * 
     * <p>Workflow path: Create empty directory → List contents → Verify empty result
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create empty directory</li>
     *   <li>{@code FileSystem.listStatus(Path)} - List empty directory contents</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify directory existence</li>
     *   <li>{@code FileSystem.isDirectory(Path)} - Verify directory type</li>
     * </ul>
     * 
     * <p>Input conditions: Create a new empty directory.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>mkdirs succeeds for new directory</li>
     *   <li>Directory exists and is recognized as a directory</li>
     *   <li>listStatus returns non-null array</li>
     *   <li>listStatus returns array with length 0 (empty)</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testListEmptyDirectory() throws IOException {
        // ARRANGE: Define path for empty directory
        Path emptyDir = new Path(testDir, "empty_directory");

        // ACT: Create empty directory using production mkdirs()
        boolean created = fs.mkdirs(emptyDir);

        // ASSERT: Directory creation succeeded
        assertTrue(created, "mkdirs should return true for new empty directory");
        assertTrue(fs.exists(emptyDir), "Empty directory should exist");
        assertTrue(fs.isDirectory(emptyDir), "Path should be a directory");

        // ACT: List contents of empty directory using production listStatus()
        FileStatus[] contents = fs.listStatus(emptyDir);

        // ASSERT: listStatus returns empty array, not null
        assertNotNull(contents, 
                "listStatus on empty directory should return non-null array");
        assertEquals(0, contents.length, 
                "listStatus on empty directory should return empty array with length 0");

        // Additional verification: Create nested empty directories
        Path nestedEmpty = new Path(emptyDir, "nested/deep/empty");
        boolean nestedCreated = fs.mkdirs(nestedEmpty);
        assertTrue(nestedCreated, "Nested empty directory creation should succeed");

        // Verify intermediate directory listing
        Path intermediateDir = new Path(emptyDir, "nested");
        FileStatus[] intermediateContents = fs.listStatus(intermediateDir);
        assertNotNull(intermediateContents, 
                "listStatus on intermediate directory should return non-null");
        assertEquals(1, intermediateContents.length, 
                "Intermediate directory should contain one entry");
        assertEquals("deep", intermediateContents[0].getPath().getName(),
                "Intermediate directory should contain 'deep' subdirectory");

        // Verify deepest empty directory
        FileStatus[] deepestContents = fs.listStatus(nestedEmpty);
        assertNotNull(deepestContents, 
                "listStatus on deepest empty directory should return non-null");
        assertEquals(0, deepestContents.length, 
                "Deepest empty directory should have zero entries");
    }

    /**
     * Tests additional directory operation edge cases.
     * 
     * <p>Workflow path: Various edge case directory operations
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create directories</li>
     *   <li>{@code FileSystem.rename(Path, Path)} - Various rename scenarios</li>
     *   <li>{@code FileSystem.exists(Path)} - Verify path existence</li>
     *   <li>{@code FileSystem.delete(Path, boolean)} - Delete directories</li>
     * </ul>
     * 
     * <p>Input conditions: Various edge case scenarios for directory operations.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>Rename non-existent directory returns false</li>
     *   <li>Rename to non-existent parent returns false</li>
     *   <li>Self-rename returns false but path remains unchanged</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDirectoryOperationsEdgeCases() throws IOException {
        // Test 1: Rename non-existent source returns false
        Path nonExistent = new Path(testDir, "non_existent_dir");
        Path validTarget = new Path(testDir, "valid_target");

        assertFalse(fs.exists(nonExistent), "Non-existent path should not exist");
        boolean renameNonExistent = fs.rename(nonExistent, validTarget);
        assertFalse(renameNonExistent, 
                "Rename of non-existent source should return false");
        assertFalse(fs.exists(validTarget), 
                "Target should not be created when source doesn't exist");

        // Test 2: Rename to path with non-existent parent returns false
        Path existingSource = new Path(testDir, "existing_source");
        assertTrue(fs.mkdirs(existingSource), "Source directory creation should succeed");

        Path invalidTarget = new Path(testDir, "non_existent_parent/target");
        boolean renameToInvalidParent = fs.rename(existingSource, invalidTarget);
        assertFalse(renameToInvalidParent, 
                "Rename to path with non-existent parent should return false");
        assertTrue(fs.exists(existingSource), 
                "Source should still exist after failed rename");

        // Test 3: Self-rename (rename to same path) behavior
        // According to HDFS behavior, renaming a path to itself returns false
        // but does not modify the source - it remains unchanged
        boolean selfRename = fs.rename(existingSource, existingSource);
        assertFalse(selfRename, 
                "Self-rename (source equals destination) should return false in HDFS");
        assertTrue(fs.exists(existingSource), 
                "Directory should still exist after self-rename attempt");

        // Test 4: Verify mkdirs creates all parent directories
        Path deepPath = new Path(testDir, "a/b/c/d/e/f/g");
        boolean deepCreate = fs.mkdirs(deepPath);
        assertTrue(deepCreate, "Deep nested mkdirs should succeed");
        assertTrue(fs.exists(deepPath), "Deep nested path should exist");

        // Verify all intermediate directories were created
        Path intermediate = testDir;
        for (String component : new String[]{"a", "b", "c", "d", "e", "f", "g"}) {
            intermediate = new Path(intermediate, component);
            assertTrue(fs.exists(intermediate), 
                    "Intermediate directory " + intermediate + " should exist");
            assertTrue(fs.isDirectory(intermediate), 
                    "Intermediate path " + intermediate + " should be a directory");
        }

        // Test 5: Delete non-empty directory with recursive=false should fail
        // In HDFS, non-recursive delete of a non-empty directory throws PathIsNotEmptyDirectoryException
        Path dirWithContent = new Path(testDir, "dir_with_content");
        assertTrue(fs.mkdirs(dirWithContent), "Directory with content creation should succeed");
        
        Path fileInDir = new Path(dirWithContent, "file.txt");
        try (FSDataOutputStream out = fs.create(fileInDir)) {
            out.write("content".getBytes(StandardCharsets.UTF_8));
        }

        // Non-recursive delete of non-empty directory should throw an exception
        IOException deleteException = org.junit.jupiter.api.Assertions.assertThrows(
                IOException.class,
                () -> fs.delete(dirWithContent, false),
                "Non-recursive delete of non-empty directory should throw IOException");
        assertTrue(deleteException.getMessage().contains("not empty") || 
                   deleteException.getClass().getSimpleName().contains("NotEmpty"),
                "Exception should indicate directory is not empty");
        
        assertTrue(fs.exists(dirWithContent), 
                "Directory should still exist after failed non-recursive delete");
        assertTrue(fs.exists(fileInDir), 
                "File in directory should still exist after failed delete");

        // Recursive delete should succeed
        boolean recursiveDelete = fs.delete(dirWithContent, true);
        assertTrue(recursiveDelete, 
                "Recursive delete of non-empty directory should succeed");
        assertFalse(fs.exists(dirWithContent), 
                "Directory should not exist after recursive delete");
    }

    /**
     * Tests listing a directory with mixed content (files and subdirectories).
     * 
     * <p>Workflow path: Create mixed content directory → List and verify all entries
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.mkdirs(Path)} - Create directories</li>
     *   <li>{@code FileSystem.create(Path)} - Create files</li>
     *   <li>{@code FileSystem.listStatus(Path)} - List mixed contents</li>
     *   <li>{@code FileStatus.isDirectory()} - Distinguish files from directories</li>
     *   <li>{@code FileStatus.isFile()} - Verify file entries</li>
     *   <li>{@code FileStatus.getLen()} - Verify file sizes</li>
     * </ul>
     * 
     * <p>Input conditions: Create a directory with multiple files and subdirectories.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>All created entries appear in listStatus</li>
     *   <li>Files are correctly identified as files</li>
     *   <li>Directories are correctly identified as directories</li>
     *   <li>File sizes match expected values</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testListMixedContents() throws IOException {
        // ARRANGE: Create a directory with mixed content
        Path mixedDir = new Path(testDir, "mixed_content");
        assertTrue(fs.mkdirs(mixedDir), "Mixed content directory creation should succeed");

        // Create multiple subdirectories
        Path subDir1 = new Path(mixedDir, "subdir1");
        Path subDir2 = new Path(mixedDir, "subdir2");
        Path subDir3 = new Path(mixedDir, "subdir3");
        assertTrue(fs.mkdirs(subDir1), "Subdirectory 1 creation should succeed");
        assertTrue(fs.mkdirs(subDir2), "Subdirectory 2 creation should succeed");
        assertTrue(fs.mkdirs(subDir3), "Subdirectory 3 creation should succeed");

        // Create multiple files with different sizes
        Path file1 = new Path(mixedDir, "file1.txt");
        Path file2 = new Path(mixedDir, "file2.dat");
        Path file3 = new Path(mixedDir, "file3.log");

        byte[] content1 = "Short content".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = "Medium length content for testing".getBytes(StandardCharsets.UTF_8);
        byte[] content3 = "This is a longer content string for the third file in our test"
                .getBytes(StandardCharsets.UTF_8);

        try (FSDataOutputStream out = fs.create(file1)) {
            out.write(content1);
        }
        try (FSDataOutputStream out = fs.create(file2)) {
            out.write(content2);
        }
        try (FSDataOutputStream out = fs.create(file3)) {
            out.write(content3);
        }

        // ACT: List mixed content directory
        FileStatus[] contents = fs.listStatus(mixedDir);

        // ASSERT: Verify total count
        assertNotNull(contents, "listStatus should not return null");
        assertEquals(6, contents.length, 
                "Directory should contain 6 entries (3 subdirs + 3 files)");

        // Count and verify files and directories
        int fileCount = 0;
        int dirCount = 0;
        boolean foundFile1 = false;
        boolean foundFile2 = false;
        boolean foundFile3 = false;
        boolean foundSubDir1 = false;
        boolean foundSubDir2 = false;
        boolean foundSubDir3 = false;

        for (FileStatus status : contents) {
            String name = status.getPath().getName();
            if (status.isDirectory()) {
                dirCount++;
                switch (name) {
                    case "subdir1":
                        foundSubDir1 = true;
                        break;
                    case "subdir2":
                        foundSubDir2 = true;
                        break;
                    case "subdir3":
                        foundSubDir3 = true;
                        break;
                }
            } else if (status.isFile()) {
                fileCount++;
                switch (name) {
                    case "file1.txt":
                        foundFile1 = true;
                        assertEquals(content1.length, status.getLen(),
                                "File1 size should match written content");
                        break;
                    case "file2.dat":
                        foundFile2 = true;
                        assertEquals(content2.length, status.getLen(),
                                "File2 size should match written content");
                        break;
                    case "file3.log":
                        foundFile3 = true;
                        assertEquals(content3.length, status.getLen(),
                                "File3 size should match written content");
                        break;
                }
            }
        }

        // Verify counts
        assertEquals(3, fileCount, "Should have 3 files");
        assertEquals(3, dirCount, "Should have 3 directories");

        // Verify all entries were found
        assertTrue(foundFile1, "Should find file1.txt");
        assertTrue(foundFile2, "Should find file2.dat");
        assertTrue(foundFile3, "Should find file3.log");
        assertTrue(foundSubDir1, "Should find subdir1");
        assertTrue(foundSubDir2, "Should find subdir2");
        assertTrue(foundSubDir3, "Should find subdir3");
    }

    /**
     * Tests rename operation preserves file attributes and content.
     * 
     * <p>Workflow path: Create file with content → Rename → Verify attributes preserved
     * 
     * <p>Production methods invoked:
     * <ul>
     *   <li>{@code FileSystem.create(Path)} - Create test file</li>
     *   <li>{@code FileSystem.rename(Path, Path)} - Rename file</li>
     *   <li>{@code FileSystem.getFileStatus(Path)} - Get file attributes</li>
     *   <li>{@code FileStatus.getReplication()} - Verify replication factor</li>
     *   <li>{@code FileStatus.getLen()} - Verify file size preserved</li>
     * </ul>
     * 
     * <p>Input conditions: Create a file with specific content, then rename it.
     * 
     * <p>Validation criteria:
     * <ul>
     *   <li>Rename succeeds</li>
     *   <li>File size is preserved</li>
     *   <li>Replication factor is preserved</li>
     *   <li>Original path no longer exists</li>
     * </ul>
     * 
     * @throws IOException if any file system operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRenamePreservesAttributes() throws IOException {
        // ARRANGE: Create file with specific content
        Path originalFile = new Path(testDir, "original_file.txt");
        byte[] testContent = "Test content for rename attribute preservation test"
                .getBytes(StandardCharsets.UTF_8);

        try (FSDataOutputStream out = fs.create(originalFile)) {
            out.write(testContent);
        }

        // Get original file attributes
        FileStatus originalStatus = fs.getFileStatus(originalFile);
        long originalLength = originalStatus.getLen();
        short originalReplication = originalStatus.getReplication();
        long originalBlockSize = originalStatus.getBlockSize();

        // ACT: Rename the file
        Path renamedFile = new Path(testDir, "renamed_file.txt");
        boolean renameResult = fs.rename(originalFile, renamedFile);

        // ASSERT: Rename succeeded
        assertTrue(renameResult, "File rename should succeed");
        assertFalse(fs.exists(originalFile), "Original file should not exist after rename");
        assertTrue(fs.exists(renamedFile), "Renamed file should exist");

        // Get renamed file attributes
        FileStatus renamedStatus = fs.getFileStatus(renamedFile);

        // ASSERT: Attributes are preserved
        assertEquals(originalLength, renamedStatus.getLen(),
                "File length should be preserved after rename");
        assertEquals(originalReplication, renamedStatus.getReplication(),
                "Replication factor should be preserved after rename");
        assertEquals(originalBlockSize, renamedStatus.getBlockSize(),
                "Block size should be preserved after rename");
        assertTrue(renamedStatus.isFile(), "Renamed path should be a file");
    }
}
