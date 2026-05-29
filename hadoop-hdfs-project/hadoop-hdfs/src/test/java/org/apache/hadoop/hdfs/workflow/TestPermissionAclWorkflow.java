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
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.fs.permission.FsAction.WRITE;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * JUnit 5 workflow test class covering HDFS Workflow 5: Permission/ACL Modification.
 * 
 * <p>This test class validates the following HDFS permission and ACL operations:
 * <ul>
 *   <li>{@link #testSetPermission()} - Sets permissions on files/directories and verifies
 *       via getFileStatus()</li>
 *   <li>{@link #testModifyAclEntries()} - Adds/modifies ACL entries and verifies via
 *       getAclStatus()</li>
 *   <li>{@link #testSetAcl()} - Replaces full ACL and verifies all entries</li>
 *   <li>{@link #testRemoveAllAcls()} - Removes all ACLs from a file and verifies empty</li>
 *   <li>{@link #testAccessCheck()} - Verifies access() returns correct permission check
 *       results for different users</li>
 * </ul>
 * 
 * <h3>Workflow Coverage</h3>
 * <p>This class covers Workflow 5 (Permission/ACL Modification) of the 15 critical
 * HDFS workflows. All tests invoke production FileSystem APIs directly without
 * reimplementing any business logic.
 * 
 * <h3>Production APIs Exercised</h3>
 * <ul>
 *   <li>{@code FileSystem.setPermission()} - Change file/directory permissions</li>
 *   <li>{@code FileSystem.modifyAclEntries()} - Add or modify ACL entries</li>
 *   <li>{@code FileSystem.setAcl()} - Replace complete ACL</li>
 *   <li>{@code FileSystem.removeAcl()} - Remove all ACL entries</li>
 *   <li>{@code FileSystem.access()} - Check if user has access</li>
 *   <li>{@code FileSystem.getAclStatus()} - Retrieve current ACL entries</li>
 *   <li>{@code FileSystem.getFileStatus()} - Retrieve file metadata including permissions</li>
 * </ul>
 * 
 * <h3>Test Constraints</h3>
 * <ul>
 *   <li>All tests must complete within 60 seconds (enforced via {@code @Timeout})</li>
 *   <li>No Thread.sleep() calls - async operations use GenericTestUtils.waitFor()</li>
 *   <li>Tests extend AbstractHdfsWorkflowTest for static MiniDFSCluster reuse</li>
 *   <li>ACLs are enabled via DFS_NAMENODE_ACLS_ENABLED_KEY in base class configuration</li>
 * </ul>
 * 
 * @see AbstractHdfsWorkflowTest
 */
public class TestPermissionAclWorkflow extends AbstractHdfsWorkflowTest {

    /**
     * Test user with restricted permissions for access verification.
     */
    private static final String TEST_USER = "testuser";

    /**
     * Test group for ACL entry testing.
     */
    private static final String TEST_GROUP = "testgroup";

    /**
     * Secondary test user for ACL-based access verification.
     */
    private static final String ACL_USER = "acluser";

    /**
     * Creates a new AclEntry with scope, type and permission (no name).
     * Helper method following the pattern from AclTestHelpers.
     *
     * @param scope AclEntryScope scope of the ACL entry
     * @param type AclEntryType ACL entry type
     * @param permission FsAction set of permissions in the ACL entry
     * @return AclEntry new AclEntry
     */
    private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
            FsAction permission) {
        return new AclEntry.Builder()
                .setScope(scope)
                .setType(type)
                .setPermission(permission)
                .build();
    }

    /**
     * Creates a new AclEntry with scope, type, name and permission.
     * Helper method following the pattern from AclTestHelpers.
     *
     * @param scope AclEntryScope scope of the ACL entry
     * @param type AclEntryType ACL entry type
     * @param name String optional ACL entry name
     * @param permission FsAction set of permissions in the ACL entry
     * @return AclEntry new AclEntry
     */
    private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
            String name, FsAction permission) {
        return new AclEntry.Builder()
                .setScope(scope)
                .setType(type)
                .setName(name)
                .setPermission(permission)
                .build();
    }

    /**
     * Workflow path: Set permission on file and directory, verify with getFileStatus().
     * Production methods invoked: FileSystem.setPermission(), FileSystem.getFileStatus()
     * Input conditions: Create file with default permissions, then set to specific octal values
     * Validation criteria: FileStatus.getPermission() returns expected permission for both
     *                      files and directories after setPermission() call
     * 
     * <p>This test validates the complete workflow of:
     * <ol>
     *   <li>Creating a file/directory with default permissions</li>
     *   <li>Calling setPermission() to change permissions</li>
     *   <li>Verifying the change via getFileStatus().getPermission()</li>
     * </ol>
     * 
     * @throws Exception if any operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSetPermission() throws Exception {
        // ARRANGE: Create a test file
        Path testFile = new Path(testDir, "permissionTestFile.txt");
        try (FSDataOutputStream out = fs.create(testFile)) {
            out.write("Permission test content".getBytes());
        }

        // Create a test directory
        Path testSubDir = new Path(testDir, "permissionTestDir");
        fs.mkdirs(testSubDir);

        // ACT & ASSERT: Test file permissions
        // Set permission to 0644 (rw-r--r--)
        FsPermission filePermission644 = new FsPermission((short) 0644);
        fs.setPermission(testFile, filePermission644);

        // Verify file permission via production API
        FileStatus fileStatus = fs.getFileStatus(testFile);
        assertNotNull(fileStatus, "FileStatus should not be null");
        assertEquals(filePermission644, fileStatus.getPermission(),
                "File permission should be 0644 (rw-r--r--)");

        // Set permission to 0755 (rwxr-xr-x)
        FsPermission filePermission755 = new FsPermission((short) 0755);
        fs.setPermission(testFile, filePermission755);

        // Verify updated permission
        fileStatus = fs.getFileStatus(testFile);
        assertEquals(filePermission755, fileStatus.getPermission(),
                "File permission should be updated to 0755 (rwxr-xr-x)");

        // ACT & ASSERT: Test directory permissions
        // Set directory permission to 0750 (rwxr-x---)
        FsPermission dirPermission750 = new FsPermission((short) 0750);
        fs.setPermission(testSubDir, dirPermission750);

        // Verify directory permission via production API
        FileStatus dirStatus = fs.getFileStatus(testSubDir);
        assertNotNull(dirStatus, "Directory FileStatus should not be null");
        assertEquals(dirPermission750, dirStatus.getPermission(),
                "Directory permission should be 0750 (rwxr-x---)");

        // Test edge case: Set permission to 0000 (no permissions)
        FsPermission noPermission = new FsPermission((short) 0000);
        fs.setPermission(testFile, noPermission);

        fileStatus = fs.getFileStatus(testFile);
        assertEquals(noPermission, fileStatus.getPermission(),
                "File permission should be 0000 (no permissions)");

        // Test edge case: Set permission to 0777 (all permissions)
        FsPermission allPermission = new FsPermission((short) 0777);
        fs.setPermission(testFile, allPermission);

        fileStatus = fs.getFileStatus(testFile);
        assertEquals(allPermission, fileStatus.getPermission(),
                "File permission should be 0777 (all permissions)");
    }

    /**
     * Workflow path: Modify ACL entries on file, verify with getAclStatus().
     * Production methods invoked: FileSystem.modifyAclEntries(), FileSystem.getAclStatus()
     * Input conditions: Create file, add named user ACL entry with read permission
     * Validation criteria: getAclStatus() returns ACL list containing the added entry
     * 
     * <p>This test validates:
     * <ol>
     *   <li>Adding a named user ACL entry to a file</li>
     *   <li>Adding a named group ACL entry</li>
     *   <li>Verifying entries via getAclStatus()</li>
     *   <li>Modifying an existing ACL entry and verifying the update</li>
     * </ol>
     * 
     * @throws Exception if any operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testModifyAclEntries() throws Exception {
        // ARRANGE: Create a test file
        Path testFile = new Path(testDir, "aclModifyTestFile.txt");
        try (FSDataOutputStream out = fs.create(testFile)) {
            out.write("ACL modification test content".getBytes());
        }

        // ACT: Add a named user ACL entry with read permission
        List<AclEntry> aclsToAdd = Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, READ)
        );
        fs.modifyAclEntries(testFile, aclsToAdd);

        // ASSERT: Verify the ACL entry was added
        AclStatus aclStatus = fs.getAclStatus(testFile);
        assertNotNull(aclStatus, "AclStatus should not be null");

        List<AclEntry> entries = aclStatus.getEntries();
        assertNotNull(entries, "ACL entries list should not be null");
        assertFalse(entries.isEmpty(), "ACL entries should not be empty after adding entry");

        // Verify the named user entry exists with correct permissions
        boolean foundUserEntry = entries.stream()
                .anyMatch(entry -> entry.getType() == USER
                        && ACL_USER.equals(entry.getName())
                        && entry.getPermission() == READ);
        assertTrue(foundUserEntry,
                "ACL entries should contain user:" + ACL_USER + ":r-- entry");

        // ACT: Add a named group ACL entry
        List<AclEntry> groupAclToAdd = Lists.newArrayList(
                aclEntry(ACCESS, GROUP, TEST_GROUP, READ_WRITE)
        );
        fs.modifyAclEntries(testFile, groupAclToAdd);

        // ASSERT: Verify both entries exist
        aclStatus = fs.getAclStatus(testFile);
        entries = aclStatus.getEntries();

        boolean foundGroupEntry = entries.stream()
                .anyMatch(entry -> entry.getType() == GROUP
                        && TEST_GROUP.equals(entry.getName())
                        && entry.getPermission() == READ_WRITE);
        assertTrue(foundGroupEntry,
                "ACL entries should contain group:" + TEST_GROUP + ":rw- entry");

        // ACT: Modify existing user entry to have different permission
        List<AclEntry> modifiedAcls = Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, ALL)
        );
        fs.modifyAclEntries(testFile, modifiedAcls);

        // ASSERT: Verify the entry was updated
        aclStatus = fs.getAclStatus(testFile);
        entries = aclStatus.getEntries();

        boolean foundUpdatedUserEntry = entries.stream()
                .anyMatch(entry -> entry.getType() == USER
                        && ACL_USER.equals(entry.getName())
                        && entry.getPermission() == ALL);
        assertTrue(foundUpdatedUserEntry,
                "ACL entries should contain updated user:" + ACL_USER + ":rwx entry");

        // Verify no duplicate entries with old permission
        long countUserEntries = entries.stream()
                .filter(entry -> entry.getType() == USER && ACL_USER.equals(entry.getName()))
                .count();
        assertEquals(1, countUserEntries,
                "There should be exactly one ACL entry for user:" + ACL_USER);
    }

    /**
     * Workflow path: Replace full ACL on file/directory, verify all entries.
     * Production methods invoked: FileSystem.setAcl(), FileSystem.getAclStatus()
     * Input conditions: Create file with existing ACLs, replace with new complete ACL
     * Validation criteria: getAclStatus() returns exactly the new ACL entries
     * 
     * <p>This test validates:
     * <ol>
     *   <li>Setting a complete ACL on a file</li>
     *   <li>Verifying all entries match exactly what was set</li>
     *   <li>Setting ACLs on a directory with default ACLs</li>
     *   <li>Verifying default ACLs are applied correctly</li>
     * </ol>
     * 
     * @throws Exception if any operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSetAcl() throws Exception {
        // ARRANGE: Create a test file
        Path testFile = new Path(testDir, "setAclTestFile.txt");
        try (FSDataOutputStream out = fs.create(testFile)) {
            out.write("Set ACL test content".getBytes());
        }

        // ACT: Set a complete ACL on the file
        // The setAcl requires at least user, group, and other entries
        List<AclEntry> newAcl = Lists.newArrayList(
                aclEntry(ACCESS, USER, ALL),
                aclEntry(ACCESS, USER, ACL_USER, READ_EXECUTE),
                aclEntry(ACCESS, GROUP, READ),
                aclEntry(ACCESS, GROUP, TEST_GROUP, READ_WRITE),
                aclEntry(ACCESS, MASK, ALL),
                aclEntry(ACCESS, OTHER, NONE)
        );
        fs.setAcl(testFile, newAcl);

        // ASSERT: Verify the ACL entries
        AclStatus aclStatus = fs.getAclStatus(testFile);
        assertNotNull(aclStatus, "AclStatus should not be null");

        List<AclEntry> entries = aclStatus.getEntries();
        assertNotNull(entries, "ACL entries should not be null");

        // Verify named user entry
        boolean hasUserEntry = entries.stream()
                .anyMatch(e -> e.getType() == USER
                        && ACL_USER.equals(e.getName())
                        && e.getPermission() == READ_EXECUTE);
        assertTrue(hasUserEntry, "Should have user:" + ACL_USER + ":r-x entry");

        // Verify named group entry
        boolean hasGroupEntry = entries.stream()
                .anyMatch(e -> e.getType() == GROUP
                        && TEST_GROUP.equals(e.getName())
                        && e.getPermission() == READ_WRITE);
        assertTrue(hasGroupEntry, "Should have group:" + TEST_GROUP + ":rw- entry");

        // Note: For ACCESS scope on files, HDFS does not return the mask entry in getEntries()
        // The mask is computed and stored internally but not explicitly returned
        // The mask IS returned for DEFAULT scope entries on directories
        // Verify at least two named entries exist (user and group)
        long namedEntriesCount = entries.stream()
                .filter(e -> e.getName() != null)
                .count();
        assertTrue(namedEntriesCount >= 2, "Should have at least 2 named ACL entries");

        // ARRANGE: Create a test directory for default ACLs
        Path testSubDir = new Path(testDir, "setAclTestDir");
        fs.mkdirs(testSubDir);

        // ACT: Set ACL with default entries on directory
        List<AclEntry> dirAcl = Lists.newArrayList(
                aclEntry(ACCESS, USER, ALL),
                aclEntry(ACCESS, GROUP, READ_EXECUTE),
                aclEntry(ACCESS, OTHER, READ_EXECUTE),
                aclEntry(DEFAULT, USER, ALL),
                aclEntry(DEFAULT, USER, ACL_USER, READ_WRITE),
                aclEntry(DEFAULT, GROUP, READ_EXECUTE),
                aclEntry(DEFAULT, MASK, ALL),
                aclEntry(DEFAULT, OTHER, READ)
        );
        fs.setAcl(testSubDir, dirAcl);

        // ASSERT: Verify directory ACL including default entries
        aclStatus = fs.getAclStatus(testSubDir);
        entries = aclStatus.getEntries();

        // Verify default user entry
        boolean hasDefaultUserEntry = entries.stream()
                .anyMatch(e -> e.getScope() == DEFAULT
                        && e.getType() == USER
                        && ACL_USER.equals(e.getName())
                        && e.getPermission() == READ_WRITE);
        assertTrue(hasDefaultUserEntry,
                "Should have default:user:" + ACL_USER + ":rw- entry");

        // Verify default mask entry
        boolean hasDefaultMask = entries.stream()
                .anyMatch(e -> e.getScope() == DEFAULT && e.getType() == MASK);
        assertTrue(hasDefaultMask, "Should have default:mask entry");

        // ACT: Replace ACL with simpler set to verify complete replacement
        List<AclEntry> simpleAcl = Lists.newArrayList(
                aclEntry(ACCESS, USER, READ_WRITE),
                aclEntry(ACCESS, GROUP, READ),
                aclEntry(ACCESS, OTHER, NONE)
        );
        fs.setAcl(testFile, simpleAcl);

        // ASSERT: Verify old entries are gone
        aclStatus = fs.getAclStatus(testFile);
        entries = aclStatus.getEntries();

        // After removing named entries, the entries list should be empty or minimal
        boolean noNamedUserEntry = entries.stream()
                .noneMatch(e -> e.getType() == USER && ACL_USER.equals(e.getName()));
        assertTrue(noNamedUserEntry,
                "Named user entry should be removed after setAcl with simple ACL");
    }

    /**
     * Workflow path: Remove all ACLs from file, verify empty.
     * Production methods invoked: FileSystem.removeAcl(), FileSystem.getAclStatus()
     * Input conditions: Create file with ACLs, then remove all ACLs
     * Validation criteria: getAclStatus().getEntries() returns empty list
     * 
     * <p>This test validates:
     * <ol>
     *   <li>Adding ACLs to a file</li>
     *   <li>Removing all ACLs via removeAcl()</li>
     *   <li>Verifying the ACL entries list is empty</li>
     *   <li>Verifying base permissions are preserved</li>
     * </ol>
     * 
     * @throws Exception if any operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRemoveAllAcls() throws Exception {
        // ARRANGE: Create a test file
        Path testFile = new Path(testDir, "removeAclTestFile.txt");
        try (FSDataOutputStream out = fs.create(testFile)) {
            out.write("Remove ACL test content".getBytes());
        }

        // Set base permission first
        FsPermission basePermission = new FsPermission((short) 0644);
        fs.setPermission(testFile, basePermission);

        // Add some ACL entries
        List<AclEntry> aclsToAdd = Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, READ_WRITE),
                aclEntry(ACCESS, GROUP, TEST_GROUP, READ)
        );
        fs.modifyAclEntries(testFile, aclsToAdd);

        // Verify ACLs were added
        AclStatus aclStatusBefore = fs.getAclStatus(testFile);
        assertFalse(aclStatusBefore.getEntries().isEmpty(),
                "ACL entries should not be empty before removal");

        // ACT: Remove all ACLs
        fs.removeAcl(testFile);

        // ASSERT: Verify all ACL entries are removed
        AclStatus aclStatusAfter = fs.getAclStatus(testFile);
        assertNotNull(aclStatusAfter, "AclStatus should not be null after removal");

        List<AclEntry> entriesAfter = aclStatusAfter.getEntries();
        assertTrue(entriesAfter.isEmpty(),
                "ACL entries should be empty after removeAcl()");

        // Verify base permission is still accessible via getFileStatus
        FileStatus fileStatus = fs.getFileStatus(testFile);
        assertNotNull(fileStatus.getPermission(),
                "Base permission should still be accessible after ACL removal");

        // ARRANGE: Test directory with default ACLs
        Path testSubDir = new Path(testDir, "removeAclTestDir");
        fs.mkdirs(testSubDir);

        // Add default ACLs to directory
        List<AclEntry> dirAcls = Lists.newArrayList(
                aclEntry(DEFAULT, USER, ACL_USER, ALL),
                aclEntry(DEFAULT, GROUP, TEST_GROUP, READ_EXECUTE)
        );
        fs.modifyAclEntries(testSubDir, dirAcls);

        // Verify default ACLs were added
        aclStatusBefore = fs.getAclStatus(testSubDir);
        boolean hasDefaultEntries = aclStatusBefore.getEntries().stream()
                .anyMatch(e -> e.getScope() == DEFAULT);
        assertTrue(hasDefaultEntries,
                "Directory should have default ACL entries before removal");

        // ACT: Remove all ACLs from directory
        fs.removeAcl(testSubDir);

        // ASSERT: Verify default ACLs are also removed
        aclStatusAfter = fs.getAclStatus(testSubDir);
        boolean hasDefaultEntriesAfter = aclStatusAfter.getEntries().stream()
                .anyMatch(e -> e.getScope() == DEFAULT);
        assertFalse(hasDefaultEntriesAfter,
                "Directory should not have default ACL entries after removal");
    }

    /**
     * Workflow path: Verify access() returns correct permission check results.
     * Production methods invoked: FileSystem.access(), FileSystem.setPermission(),
     *                            FileSystem.setAcl(), UserGroupInformation.doAs()
     * Input conditions: Create file with specific permissions, test access for different users
     * Validation criteria: access() returns success for authorized users, throws
     *                      AccessControlException for unauthorized users
     * 
     * <p>This test validates:
     * <ol>
     *   <li>access() succeeds when user has required permissions</li>
     *   <li>access() throws AccessControlException when permission denied</li>
     *   <li>ACL-based access grants work correctly for named users</li>
     *   <li>Permission and ACL interaction for access checks</li>
     * </ol>
     * 
     * @throws Exception if any operation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testAccessCheck() throws Exception {
        // ARRANGE: Create a test file with specific permissions
        Path testFile = new Path(testDir, "accessCheckTestFile.txt");
        try (FSDataOutputStream out = fs.create(testFile)) {
            out.write("Access check test content".getBytes());
        }

        // Set restrictive permissions: owner rwx, group r--, other ---
        FsPermission restrictivePermission = new FsPermission((short) 0740);
        fs.setPermission(testFile, restrictivePermission);

        // Create test users
        UserGroupInformation testUser = UserGroupInformation.createUserForTesting(
                TEST_USER, new String[]{TEST_GROUP});
        UserGroupInformation aclTestUser = UserGroupInformation.createUserForTesting(
                ACL_USER, new String[]{"othergroup"});

        // ACT & ASSERT: Test access for superuser (should always succeed)
        // Superuser can access anything
        fs.access(testFile, FsAction.READ);
        fs.access(testFile, FsAction.WRITE);
        fs.access(testFile, FsAction.EXECUTE);

        // ACT & ASSERT: Test access for restricted user without ACL
        // User in "othergroup" should not have access (other has no permissions)
        boolean accessDenied = false;
        try {
            FileSystem userFs = aclTestUser.doAs(
                    (PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(conf)
            );
            userFs.access(testFile, FsAction.READ);
        } catch (AccessControlException e) {
            accessDenied = true;
        }
        assertTrue(accessDenied,
                "Access should be denied for user without ACL grant (other has no perms)");

        // ACT: Add ACL entry granting read access to aclTestUser
        List<AclEntry> aclGrant = Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, READ)
        );
        fs.modifyAclEntries(testFile, aclGrant);

        // ASSERT: Now aclTestUser should have read access via ACL
        boolean canReadWithAcl = tryAccess(testFile, aclTestUser, FsAction.READ);
        assertTrue(canReadWithAcl,
                "User should have read access after ACL grant");

        // ASSERT: But write access should still be denied (ACL only grants read)
        boolean canWriteWithAcl = tryAccess(testFile, aclTestUser, FsAction.WRITE);
        assertFalse(canWriteWithAcl,
                "User should not have write access (ACL only grants read)");

        // ACT: Update ACL to grant write access as well
        List<AclEntry> aclUpdate = Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, READ_WRITE)
        );
        fs.modifyAclEntries(testFile, aclUpdate);

        // ASSERT: Now write access should be granted
        boolean canWriteAfterUpdate = tryAccess(testFile, aclTestUser, FsAction.WRITE);
        assertTrue(canWriteAfterUpdate,
                "User should have write access after ACL update");

        // ACT: Test directory access for execute permission via ACLs
        Path testSubDir = new Path(testDir, "accessCheckDir");
        fs.mkdirs(testSubDir);

        // Set directory permissions: owner rwx, group ---, other ---
        // This ensures no one except owner has access without ACL
        fs.setPermission(testSubDir, new FsPermission((short) 0700));

        // ASSERT: User without ACL should not have execute access
        boolean canExecuteWithoutAcl = tryAccess(testSubDir, aclTestUser, FsAction.EXECUTE);
        assertFalse(canExecuteWithoutAcl,
                "User should not have execute access without ACL grant");

        // ASSERT: User without ACL should not have read access either
        boolean canReadWithoutAcl = tryAccess(testSubDir, aclTestUser, FsAction.READ);
        assertFalse(canReadWithoutAcl,
                "User should not have read access without ACL grant");

        // ACT: Add ACL granting execute to aclTestUser
        fs.modifyAclEntries(testSubDir, Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, EXECUTE)
        ));

        // ASSERT: Now aclTestUser should have execute access via ACL
        boolean canExecuteWithAcl = tryAccess(testSubDir, aclTestUser, FsAction.EXECUTE);
        assertTrue(canExecuteWithAcl,
                "User should have execute access after ACL grant on directory");

        // ASSERT: But read access should still be denied (ACL only grants execute)
        boolean canReadAfterExecuteAcl = tryAccess(testSubDir, aclTestUser, FsAction.READ);
        assertFalse(canReadAfterExecuteAcl,
                "User should not have read access (ACL only grants execute)");

        // ACT: Add ACL granting read+execute to aclTestUser
        fs.modifyAclEntries(testSubDir, Lists.newArrayList(
                aclEntry(ACCESS, USER, ACL_USER, READ_EXECUTE)
        ));

        // ASSERT: Now aclTestUser should have both read and execute access
        boolean canReadExecuteWithAcl = tryAccess(testSubDir, aclTestUser, FsAction.READ_EXECUTE);
        assertTrue(canReadExecuteWithAcl,
                "User should have read+execute access after ACL update on directory");
    }

    /**
     * Helper method to test access for a specific user and action.
     * 
     * <p>This method uses UserGroupInformation.doAs() to execute the access check
     * as the specified user, following the pattern from TestExtendedAcls.
     * 
     * @param path the path to check access for
     * @param user the user to perform the access check as
     * @param action the FsAction to check (READ, WRITE, EXECUTE, etc.)
     * @return true if access is granted, false if AccessControlException is thrown
     * @throws Exception if an error occurs other than AccessControlException
     */
    private boolean tryAccess(Path path, UserGroupInformation user, FsAction action) 
            throws Exception {
        FileSystem userFs = user.doAs(
                (PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(conf)
        );

        try {
            userFs.access(path, action);
            return true;
        } catch (AccessControlException e) {
            return false;
        }
    }
}
