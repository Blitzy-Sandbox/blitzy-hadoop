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
package org.apache.hadoop.conf.workflow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract JUnit 5 base class for Configuration workflow tests (Workflow 14).
 * 
 * <p>This class provides shared Configuration fixtures for workflow tests that
 * validate Hadoop's Configuration loading, override, and substitution behavior.
 * It manages the test lifecycle with static setup/teardown and per-test
 * Configuration isolation.</p>
 * 
 * <h3>Design Principles</h3>
 * <ul>
 *   <li><b>Configuration Isolation:</b> Each test receives a fresh Configuration
 *       instance created via {@code new Configuration(false)} to ensure no default
 *       resources are loaded, providing clean test isolation.</li>
 *   <li><b>No MiniCluster Required:</b> Configuration tests operate entirely in-memory
 *       and do not require any cluster infrastructure, making them fast and lightweight.</li>
 *   <li><b>XML Generation Utilities:</b> Provides helper methods for generating XML
 *       configuration files following the patterns established in TestConfiguration.java.</li>
 * </ul>
 * 
 * <h3>Inherited Fixtures</h3>
 * <ul>
 *   <li>{@link #conf} - Configuration instance for each test (reset in @BeforeEach)</li>
 *   <li>{@link #out} - BufferedWriter for XML config file generation</li>
 *   <li>{@link #testConfigFile} - Per-test unique config file reference</li>
 *   <li>{@link #tempDir} - Shared temp directory injected by JUnit 5</li>
 * </ul>
 * 
 * <h3>Usage</h3>
 * <pre>{@code
 * public class TestConfigurationLoadingWorkflow extends AbstractCommonWorkflowTest {
 *     @Test
 *     void testXmlResourceLoading() throws Exception {
 *         createConfigFile();
 *         startConfig();
 *         appendProperty("test.key", "test.value");
 *         endConfig();
 *         
 *         conf.addResource(new org.apache.hadoop.fs.Path(testConfigFile.toURI()));
 *         assertEquals("test.value", conf.get("test.key"));
 *     }
 * }
 * }</pre>
 *
 * @see org.apache.hadoop.conf.Configuration
 */
public abstract class AbstractCommonWorkflowTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCommonWorkflowTest.class);

    /**
     * Shared temporary directory for all tests in this class.
     * Automatically created and cleaned up by JUnit 5.
     */
    @TempDir
    protected static Path tempDir;

    /**
     * Configuration instance for the current test.
     * Created fresh in {@link #setUp(TestInfo)} with no default resources loaded.
     */
    protected Configuration conf;

    /**
     * BufferedWriter for generating XML configuration files.
     * Opened by {@link #createConfigFile()} and closed by {@link #endConfig()} or {@link #tearDown()}.
     */
    protected BufferedWriter out;

    /**
     * Per-test configuration file.
     * Path is unique based on test method name to ensure test isolation.
     */
    protected File testConfigFile;

    /**
     * Initializes static resources for the test class.
     * Called once before any test methods in the class are executed.
     */
    @BeforeAll
    static void setUpClass() {
        LOG.info("Starting Configuration workflow test class");
        LOG.debug("Temporary directory for tests: {}", tempDir);
    }

    /**
     * Cleans up static resources after all tests in the class have completed.
     * Called once after all test methods in the class have been executed.
     */
    @AfterAll
    static void tearDownClass() {
        LOG.info("Completed Configuration workflow test class");
    }

    /**
     * Sets up the test environment before each test method.
     * 
     * <p>Creates a fresh Configuration instance with no default resources loaded,
     * ensuring each test starts with a clean slate. Also initializes the per-test
     * configuration file path based on the test method name.</p>
     *
     * @param testInfo JUnit 5 TestInfo providing access to test metadata
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        // Create fresh Configuration with no default resources for clean test isolation
        conf = new Configuration(false);
        
        // Generate unique config file name based on test method
        String testName = testInfo.getDisplayName();
        // Sanitize test name for file system (remove parentheses and special chars)
        String sanitizedName = testName.replaceAll("[^a-zA-Z0-9_-]", "_");
        testConfigFile = new File(tempDir.toFile(), sanitizedName + ".xml");
        
        LOG.debug("Test setup complete for: {}", testName);
        LOG.debug("Test config file: {}", testConfigFile.getAbsolutePath());
    }

    /**
     * Cleans up resources after each test method.
     * 
     * <p>Closes the BufferedWriter if open, deletes the test configuration file
     * if it exists, and nulls out the Configuration reference for garbage collection.</p>
     */
    @AfterEach
    void tearDown() {
        // Close BufferedWriter if open
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                LOG.warn("Error closing BufferedWriter: {}", e.getMessage());
            }
            out = null;
        }
        
        // Delete test config file if exists
        if (testConfigFile != null && testConfigFile.exists()) {
            boolean deleted = testConfigFile.delete();
            if (!deleted) {
                LOG.warn("Failed to delete test config file: {}", testConfigFile.getAbsolutePath());
            }
            testConfigFile = null;
        }
        
        // Clear Configuration reference for garbage collection
        conf = null;
        
        LOG.debug("Test teardown complete");
    }

    /**
     * Creates and opens a BufferedWriter for the test configuration file.
     * 
     * <p>This method must be called before using {@link #startConfig()},
     * {@link #appendProperty(String, String)}, or {@link #endConfig()}.</p>
     *
     * @return the File reference to the created configuration file
     * @throws IOException if the file cannot be created or opened for writing
     */
    protected File createConfigFile() throws IOException {
        out = new BufferedWriter(new FileWriter(testConfigFile));
        LOG.debug("Created config file writer: {}", testConfigFile.getAbsolutePath());
        return testConfigFile;
    }

    /**
     * Writes the XML declaration and opening configuration tag.
     * 
     * <p>Must be called after {@link #createConfigFile()} and before
     * any {@link #appendProperty(String, String)} calls.</p>
     *
     * @throws IOException if writing to the file fails
     */
    protected void startConfig() throws IOException {
        out.write("<?xml version=\"1.0\"?>\n");
        out.write("<configuration>\n");
    }

    /**
     * Writes the closing configuration tag and closes the writer.
     * 
     * <p>Must be called after all properties have been written.
     * This method flushes and closes the BufferedWriter.</p>
     *
     * @throws IOException if writing to the file fails or closing fails
     */
    protected void endConfig() throws IOException {
        out.write("</configuration>\n");
        out.flush();
        out.close();
        out = null;  // Mark as closed to prevent double-close in tearDown
    }

    /**
     * Appends a property element to the configuration XML file.
     * 
     * <p>Writes a standard property element with name and value.</p>
     *
     * @param name the property name
     * @param val the property value
     * @throws IOException if writing to the file fails
     */
    protected void appendProperty(String name, String val) throws IOException {
        appendProperty(name, val, false);
    }

    /**
     * Appends a property element to the configuration XML file with optional final flag.
     * 
     * <p>Writes a property element with name, value, and optionally the final tag.
     * Final properties cannot be overridden by subsequent configuration resources.</p>
     *
     * @param name the property name
     * @param val the property value
     * @param isFinal if true, marks the property as final (cannot be overridden)
     * @throws IOException if writing to the file fails
     */
    protected void appendProperty(String name, String val, boolean isFinal) throws IOException {
        out.write("<property>");
        out.write("<name>");
        out.write(name);
        out.write("</name>");
        out.write("<value>");
        out.write(val);
        out.write("</value>");
        if (isFinal) {
            out.write("<final>true</final>");
        }
        out.write("</property>\n");
    }

    /**
     * Appends a property element with source tracking information.
     * 
     * <p>Writes a property element with name, value, optional final flag,
     * and source tracking elements for debugging configuration provenance.</p>
     *
     * @param name the property name
     * @param val the property value
     * @param isFinal if true, marks the property as final
     * @param sources optional source identifiers for tracking where the property came from
     * @throws IOException if writing to the file fails
     */
    protected void appendProperty(String name, String val, boolean isFinal, String... sources) 
            throws IOException {
        out.write("<property>");
        out.write("<name>");
        out.write(name);
        out.write("</name>");
        out.write("<value>");
        out.write(val);
        out.write("</value>");
        if (isFinal) {
            out.write("<final>true</final>");
        }
        for (String source : sources) {
            out.write("<source>");
            out.write(source);
            out.write("</source>");
        }
        out.write("</property>\n");
    }

    /**
     * Creates a secondary configuration file with a unique name.
     * 
     * <p>Useful for tests that need to load multiple configuration resources
     * to test overlay and override behavior.</p>
     *
     * @param suffix a suffix to append to the base test name for uniqueness
     * @return a new File reference in the temp directory
     * @throws IOException if the file cannot be created
     */
    protected File createSecondaryConfigFile(String suffix) throws IOException {
        String baseName = testConfigFile.getName();
        String nameWithoutExt = baseName.substring(0, baseName.lastIndexOf('.'));
        return new File(tempDir.toFile(), nameWithoutExt + "_" + suffix + ".xml");
    }

    /**
     * Gets the Hadoop Path reference to the test configuration file.
     * 
     * <p>Converts the java.io.File to an org.apache.hadoop.fs.Path for use
     * with Configuration.addResource() methods.</p>
     *
     * @return the Hadoop Path to the test configuration file
     */
    protected org.apache.hadoop.fs.Path getConfigPath() {
        return new org.apache.hadoop.fs.Path(testConfigFile.toURI());
    }

    /**
     * Gets the Hadoop Path reference to a specified file.
     *
     * @param file the file to convert to a Hadoop Path
     * @return the Hadoop Path to the specified file
     */
    protected org.apache.hadoop.fs.Path getConfigPath(File file) {
        return new org.apache.hadoop.fs.Path(file.toURI());
    }
}
