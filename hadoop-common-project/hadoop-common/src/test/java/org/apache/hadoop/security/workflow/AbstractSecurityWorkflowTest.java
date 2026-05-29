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
package org.apache.hadoop.security.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Abstract base class for security workflow tests providing static MiniKDC
 * lifecycle management.
 * 
 * <p>This class provides a shared MiniKDC instance that is initialized once
 * before all tests in subclasses run and shut down after all tests complete.
 * This approach follows the static cluster reuse pattern to meet the 45-minute
 * CI budget requirement, as MiniKDC initialization takes approximately 5 seconds.
 * 
 * <h3>Lifecycle Management:</h3>
 * <ul>
 *   <li>{@code @BeforeAll}: Initializes MiniKDC with default configuration,
 *       creates a test principal and keytab file, and configures Hadoop
 *       security for Kerberos authentication.</li>
 *   <li>{@code @AfterAll}: Stops the MiniKDC and cleans up resources.</li>
 *   <li>{@code @BeforeEach}: Resets UserGroupInformation cached state to ensure
 *       test isolation.</li>
 *   <li>{@code @AfterEach}: Additional cleanup hook for subclass extension.</li>
 * </ul>
 * 
 * <h3>Protected Fields Available to Subclasses:</h3>
 * <ul>
 *   <li>{@code kdc} - The MiniKdc instance for creating additional principals</li>
 *   <li>{@code keytabFile} - The keytab file containing the test principal</li>
 *   <li>{@code principal} - The test principal name (without realm)</li>
 *   <li>{@code conf} - Hadoop Configuration with Kerberos authentication enabled</li>
 * </ul>
 * 
 * <h3>Usage Pattern:</h3>
 * <pre>{@code
 * public class TestKerberosAuthenticationWorkflow extends AbstractSecurityWorkflowTest {
 *     @Test
 *     void testLoginFromKeytab() throws Exception {
 *         UserGroupInformation.loginUserFromKeytab(getPrincipal(), 
 *             getKeytabFile().getPath());
 *         UserGroupInformation ugi = UserGroupInformation.getLoginUser();
 *         assertTrue(ugi.isFromKeytab());
 *     }
 * }
 * }</pre>
 * 
 * <p>This class follows patterns established in:
 * <ul>
 *   <li>{@code KerberosSecurityTestcase} - Base class pattern for MiniKDC</li>
 *   <li>{@code TestUGILoginFromKeytab} - MiniKDC usage with @TempDir</li>
 * </ul>
 * 
 * @see org.apache.hadoop.minikdc.MiniKdc
 * @see org.apache.hadoop.minikdc.KerberosSecurityTestcase
 * @see org.apache.hadoop.security.UserGroupInformation
 */
public abstract class AbstractSecurityWorkflowTest {

  private static final Logger LOG = 
      LoggerFactory.getLogger(AbstractSecurityWorkflowTest.class);

  /**
   * Default test principal name used for Kerberos authentication tests.
   * This principal is created in the MiniKDC during static setup.
   */
  private static final String DEFAULT_PRINCIPAL_NAME = "testuser";

  /**
   * Default keytab filename used to store the test principal's credentials.
   */
  private static final String DEFAULT_KEYTAB_FILENAME = "testuser.keytab";

  /**
   * The MiniKdc instance providing an embedded Kerberos KDC.
   * Initialized in {@link #setUpClass(Path)} and stopped in {@link #tearDownClass()}.
   * Subclasses can use this to create additional principals via
   * {@link MiniKdc#createPrincipal(File, String...)}.
   */
  protected static MiniKdc kdc;

  /**
   * The keytab file containing credentials for the test principal.
   * Created during static setup via {@link MiniKdc#createPrincipal(File, String...)}.
   * Use {@link File#getPath()} to get the path for 
   * {@link UserGroupInformation#loginUserFromKeytab(String, String)}.
   */
  protected static File keytabFile;

  /**
   * The test principal name (without the Kerberos realm).
   * The full principal name is {@code principal@REALM} where REALM is
   * obtained from {@link MiniKdc#getRealm()}.
   */
  protected static String principal;

  /**
   * Hadoop Configuration pre-configured with Kerberos authentication enabled.
   * The configuration has {@code hadoop.security.authentication} set to "kerberos"
   * and is applied to UserGroupInformation before tests run.
   */
  protected static Configuration conf;

  /**
   * Temporary directory for MiniKDC work files.
   * Injected by JUnit 5 via @TempDir annotation and used as the MiniKDC work directory.
   * JUnit 5 handles cleanup of this directory after tests complete.
   */
  @TempDir
  protected static Path workDir;

  /**
   * Initializes the static MiniKDC environment before any tests run.
   * 
   * <p>This method performs the following setup steps:
   * <ol>
   *   <li>Creates a default MiniKDC configuration via {@link MiniKdc#createConf()}</li>
   *   <li>Initializes the MiniKDC with the work directory from @TempDir</li>
   *   <li>Starts the KDC server</li>
   *   <li>Creates the test principal and keytab file</li>
   *   <li>Configures Hadoop security for Kerberos authentication</li>
   *   <li>Applies the configuration to UserGroupInformation</li>
   * </ol>
   * 
   * <p>The initialization takes approximately 5 seconds, which is amortized across
   * all test methods in the subclass through static reuse.
   * 
   * @param tempDir the temporary directory injected by JUnit 5 for MiniKDC files
   * @throws Exception if MiniKDC initialization, principal creation, or 
   *         configuration fails
   */
  @BeforeAll
  static void setUpClass(@TempDir Path tempDir) throws Exception {
    LOG.info("Initializing MiniKDC for security workflow tests...");
    long startTime = System.currentTimeMillis();

    // Store the work directory for subclass access
    workDir = tempDir;

    // Create MiniKDC configuration with default settings
    Properties kdcConf = MiniKdc.createConf();

    // Initialize and start MiniKDC
    File workDirFile = workDir.toFile();
    kdc = new MiniKdc(kdcConf, workDirFile);
    kdc.start();
    LOG.info("MiniKDC started with realm: {}", kdc.getRealm());

    // Create test principal and keytab file
    principal = DEFAULT_PRINCIPAL_NAME;
    keytabFile = new File(workDirFile, DEFAULT_KEYTAB_FILENAME);
    kdc.createPrincipal(keytabFile, principal);
    LOG.info("Created principal '{}' with keytab at: {}", 
        principal, keytabFile.getAbsolutePath());

    // Configure Hadoop for Kerberos authentication
    conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, 
        "kerberos");
    
    // Apply configuration to UserGroupInformation
    UserGroupInformation.setConfiguration(conf);
    
    // Enable immediate renewal for test scenarios
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);

    long elapsed = System.currentTimeMillis() - startTime;
    LOG.info("MiniKDC initialization completed in {} ms", elapsed);
  }

  /**
   * Stops the MiniKDC and cleans up resources after all tests complete.
   * 
   * <p>This method safely stops the MiniKDC if it was successfully initialized.
   * The work directory cleanup is handled automatically by JUnit 5's @TempDir.
   */
  @AfterAll
  static void tearDownClass() {
    LOG.info("Shutting down MiniKDC...");
    if (kdc != null) {
      try {
        kdc.stop();
        LOG.info("MiniKDC stopped successfully");
      } catch (Exception e) {
        LOG.warn("Error stopping MiniKDC", e);
      } finally {
        kdc = null;
      }
    }
    
    // Reset static fields
    keytabFile = null;
    principal = null;
    conf = null;
  }

  /**
   * Resets UserGroupInformation state before each test to ensure isolation.
   * 
   * <p>This method clears any cached UGI state including:
   * <ul>
   *   <li>Login user</li>
   *   <li>Current user</li>
   *   <li>Authentication method cache</li>
   * </ul>
   * 
   * <p>After reset, it re-applies the Kerberos configuration to ensure
   * each test starts with a clean, properly configured UGI state.
   */
  @BeforeEach
  void setUpTest() {
    LOG.debug("Resetting UserGroupInformation state before test");
    
    // Reset UGI to clear any cached authentication state
    UserGroupInformation.reset();
    
    // Re-apply the Kerberos configuration after reset
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
  }

  /**
   * Cleanup hook called after each test method.
   * 
   * <p>This method performs any necessary per-test cleanup. The base implementation
   * resets the UGI state to prevent test pollution. Subclasses may override this
   * method to add additional cleanup steps, but should call {@code super.tearDownTest()}
   * to maintain proper cleanup behavior.
   */
  @AfterEach
  void tearDownTest() {
    LOG.debug("Cleaning up after test");
    
    // Reset UGI to clear any authentication state from this test
    UserGroupInformation.reset();
  }

  // ========================================================================
  // Getter methods for protected fields
  // ========================================================================

  /**
   * Returns the MiniKdc instance for creating additional principals.
   * 
   * <p>Subclasses can use this to create test-specific principals via:
   * <pre>{@code
   * File newKeytab = new File(workDir.toFile(), "newprincipal.keytab");
   * getKdc().createPrincipal(newKeytab, "newprincipal");
   * }</pre>
   * 
   * @return the MiniKdc instance, or null if not initialized
   */
  protected MiniKdc getKdc() {
    return kdc;
  }

  /**
   * Returns the keytab file containing the test principal's credentials.
   * 
   * <p>Use this file path with 
   * {@link UserGroupInformation#loginUserFromKeytab(String, String)}:
   * <pre>{@code
   * UserGroupInformation.loginUserFromKeytab(getPrincipal(), 
   *     getKeytabFile().getPath());
   * }</pre>
   * 
   * @return the keytab File object, or null if not created
   */
  protected File getKeytabFile() {
    return keytabFile;
  }

  /**
   * Returns the test principal name without the Kerberos realm.
   * 
   * <p>To get the full principal name with realm, use:
   * <pre>{@code
   * String fullPrincipal = getPrincipal() + "@" + getKdc().getRealm();
   * }</pre>
   * 
   * @return the principal name (e.g., "testuser"), or null if not created
   */
  protected String getPrincipal() {
    return principal;
  }

  /**
   * Returns the Hadoop Configuration with Kerberos authentication enabled.
   * 
   * <p>This configuration has {@code hadoop.security.authentication} set to
   * "kerberos" and has been applied to UserGroupInformation. Subclasses may
   * add additional configuration properties as needed for their tests.
   * 
   * @return the Configuration object, or null if not initialized
   */
  protected Configuration getConf() {
    return conf;
  }

  /**
   * Returns the full principal name including the Kerberos realm.
   * 
   * <p>This is a convenience method that combines the principal name with
   * the MiniKDC realm. The format is {@code principal@REALM}.
   * 
   * @return the full principal name with realm, or null if KDC not initialized
   */
  protected String getFullPrincipal() {
    if (kdc == null || principal == null) {
      return null;
    }
    return principal + "@" + kdc.getRealm();
  }

  /**
   * Returns the Kerberos realm from the MiniKDC.
   * 
   * <p>This is a convenience method to access the KDC's realm without
   * directly calling {@code getKdc().getRealm()}.
   * 
   * @return the Kerberos realm (e.g., "EXAMPLE.COM"), or null if KDC not initialized
   */
  protected String getRealm() {
    if (kdc == null) {
      return null;
    }
    return kdc.getRealm();
  }

  /**
   * Creates an additional principal and keytab for testing.
   * 
   * <p>This convenience method allows subclasses to create additional
   * principals beyond the default test principal. The keytab file is
   * created in the work directory.
   * 
   * <p>Example usage:
   * <pre>{@code
   * File extraKeytab = createAdditionalPrincipal("serviceuser", "serviceuser.keytab");
   * UserGroupInformation.loginUserFromKeytab("serviceuser", extraKeytab.getPath());
   * }</pre>
   * 
   * @param principalName the name of the principal to create (without realm)
   * @param keytabFileName the name of the keytab file to create
   * @return the created keytab File
   * @throws Exception if principal or keytab creation fails
   */
  protected File createAdditionalPrincipal(String principalName, 
      String keytabFileName) throws Exception {
    if (kdc == null) {
      throw new IllegalStateException("MiniKDC not initialized");
    }
    if (workDir == null) {
      throw new IllegalStateException("Work directory not initialized");
    }
    
    File newKeytab = new File(workDir.toFile(), keytabFileName);
    kdc.createPrincipal(newKeytab, principalName);
    LOG.info("Created additional principal '{}' with keytab at: {}", 
        principalName, newKeytab.getAbsolutePath());
    return newKeytab;
  }
}
