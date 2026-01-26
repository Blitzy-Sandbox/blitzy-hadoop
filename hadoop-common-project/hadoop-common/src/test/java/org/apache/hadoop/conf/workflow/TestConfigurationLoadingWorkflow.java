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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit 5 workflow test class for Hadoop Configuration loading (Workflow 14).
 * 
 * <p>This test class validates the critical Configuration loading workflows including:
 * <ul>
 *   <li>XML resource loading with property retrieval</li>
 *   <li>Programmatic override of XML values</li>
 *   <li>Variable substitution (${property}, ${env.VAR}, ${java.version})</li>
 *   <li>Final property protection from override</li>
 *   <li>Missing property default value handling</li>
 * </ul>
 * 
 * <h3>Production Function Exclusivity</h3>
 * <p>All tests invoke production Configuration methods directly (addResource, get, set,
 * getRaw, getInt, getBoolean, getPropertySources) to validate real behavior per
 * Production Function Exclusivity requirement. No internal methods are mocked or stubbed.</p>
 * 
 * <h3>Test Isolation</h3>
 * <p>Each test method receives a fresh Configuration instance created with 
 * {@code new Configuration(false)} to ensure no default resources are loaded,
 * providing complete test isolation.</p>
 * 
 * <h3>Coverage Target</h3>
 * <p>This class targets 80% line coverage on the org.apache.hadoop.conf package
 * for Configuration loading workflows.</p>
 *
 * @see org.apache.hadoop.conf.Configuration
 * @see AbstractCommonWorkflowTest
 */
public class TestConfigurationLoadingWorkflow extends AbstractCommonWorkflowTest {

    /**
     * Tests XML resource loading with property retrieval workflow.
     * 
     * <p>Workflow path: XML resource loading with property retrieval</p>
     * <p>Production methods invoked:</p>
     * <ul>
     *   <li>{@link Configuration#addResource(Path)} - Load XML configuration file</li>
     *   <li>{@link Configuration#get(String)} - Retrieve property value</li>
     *   <li>{@link Configuration#getPropertySources(String)} - Retrieve property source tracking</li>
     * </ul>
     * <p>Input conditions: XML file with single property "test.property" = "test.value"</p>
     * <p>Validation criteria:</p>
     * <ul>
     *   <li>Property value matches expected value</li>
     *   <li>Property source tracking includes the config file path</li>
     *   <li>Multiple properties can be loaded from same XML</li>
     * </ul>
     *
     * @throws IOException if configuration file creation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testXmlResourceLoading() throws IOException {
        // ARRANGE: Create XML configuration file with properties
        createConfigFile();
        startConfig();
        appendProperty("test.property", "test.value");
        appendProperty("test.another", "another.value");
        appendProperty("test.number", "42");
        endConfig();
        
        // ACT: Load configuration from XML file using production API
        Path configPath = getConfigPath();
        conf.addResource(configPath);
        
        // Force properties to load (lazy initialization)
        String value = conf.get("test.property");
        
        // ASSERT: Validate property retrieval via production API
        assertEquals("test.value", value,
            "Property value should match what was set in XML");
        
        // Validate second property also loaded
        assertEquals("another.value", conf.get("test.another"),
            "Multiple properties from same XML should be loaded");
        
        // Validate numeric string property
        assertEquals("42", conf.get("test.number"),
            "Numeric string property should be loaded correctly");
        
        // Validate property source tracking via production API
        String[] sources = conf.getPropertySources("test.property");
        assertNotNull(sources, "Property sources should not be null");
        assertTrue(sources.length > 0, "Property sources should contain at least one entry");
        
        // The source should contain the config file path
        boolean foundConfigSource = false;
        for (String source : sources) {
            if (source.contains(testConfigFile.getName())) {
                foundConfigSource = true;
                break;
            }
        }
        assertTrue(foundConfigSource,
            "Property sources should include the config file path");
    }

    /**
     * Tests programmatic set overrides XML values workflow.
     * 
     * <p>Workflow path: Programmatic set overrides XML values</p>
     * <p>Production methods invoked:</p>
     * <ul>
     *   <li>{@link Configuration#addResource(Path)} - Load XML configuration file</li>
     *   <li>{@link Configuration#set(String, String)} - Programmatically set property</li>
     *   <li>{@link Configuration#get(String)} - Retrieve property value</li>
     * </ul>
     * <p>Input conditions:</p>
     * <ul>
     *   <li>XML file with property "override.test" = "original.value"</li>
     *   <li>Programmatic set of "override.test" to "new.value"</li>
     * </ul>
     * <p>Validation criteria:</p>
     * <ul>
     *   <li>Programmatic value takes precedence over XML value</li>
     *   <li>Both pre-load and post-load programmatic overrides work correctly</li>
     * </ul>
     *
     * @throws IOException if configuration file creation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testProgrammaticOverride() throws IOException {
        // ARRANGE: Create XML configuration file with initial property value
        createConfigFile();
        startConfig();
        appendProperty("override.test", "original.value");
        appendProperty("no.override", "stay.same");
        appendProperty("pre.set.test", "xml.value");
        endConfig();
        
        // Set a property programmatically BEFORE loading XML
        conf.set("pre.set.test", "programmatic.before.load");
        
        // ACT: Load XML configuration
        Path configPath = getConfigPath();
        conf.addResource(configPath);
        
        // Force property load
        conf.get("override.test");
        
        // ASSERT: Verify original value from XML is loaded initially
        assertEquals("original.value", conf.get("override.test"),
            "Original value from XML should be loaded");
        
        // ACT: Override with programmatic set - production API
        conf.set("override.test", "new.value");
        
        // ASSERT: Verify programmatic override takes precedence
        assertEquals("new.value", conf.get("override.test"),
            "Programmatic set should override XML value");
        
        // Verify non-overridden property remains unchanged
        assertEquals("stay.same", conf.get("no.override"),
            "Non-overridden property should retain original value");
        
        // Verify pre-load set behavior - programmatic value before XML load
        // In Configuration, addResource lazily loads, and programmatic sets
        // before or after should override
        assertEquals("programmatic.before.load", conf.get("pre.set.test"),
            "Programmatic set before XML load should be preserved");
        
        // Additional verification: Multiple overrides should use latest value
        conf.set("override.test", "final.value");
        assertEquals("final.value", conf.get("override.test"),
            "Multiple programmatic sets should use the latest value");
    }

    /**
     * Tests ${variable} substitution in property values workflow.
     * 
     * <p>Workflow path: ${variable} substitution in property values</p>
     * <p>Production methods invoked:</p>
     * <ul>
     *   <li>{@link Configuration#addResource(Path)} - Load XML configuration file</li>
     *   <li>{@link Configuration#get(String)} - Retrieve property with substitution</li>
     *   <li>{@link Configuration#getRaw(String)} - Retrieve unexpanded raw value</li>
     * </ul>
     * <p>Input conditions:</p>
     * <ul>
     *   <li>Properties with ${other.property} syntax</li>
     *   <li>Properties with ${java.version} system property reference</li>
     *   <li>Chained substitutions</li>
     * </ul>
     * <p>Validation criteria:</p>
     * <ul>
     *   <li>conf.get() returns expanded/substituted value</li>
     *   <li>conf.getRaw() returns unexpanded raw value</li>
     *   <li>System property substitution works</li>
     *   <li>Undefined variables remain as-is in output</li>
     * </ul>
     *
     * @throws IOException if configuration file creation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testVariableSubstitution() throws IOException {
        // ARRANGE: Create XML configuration file with variable references
        createConfigFile();
        startConfig();
        // Base property to be referenced
        appendProperty("base.dir", "/user/hadoop");
        // Property with simple variable reference
        appendProperty("data.dir", "${base.dir}/data");
        // Property with chained variables
        appendProperty("output.dir", "${data.dir}/output");
        // Property with system property reference (java.version is always available)
        appendProperty("java.info", "Java Version: ${java.version}");
        // Property with undefined variable (should remain as-is)
        appendProperty("undefined.ref", "${undefined.property}/path");
        // Property with multiple variable references
        appendProperty("multi.ref", "${base.dir}/${base.dir}");
        // Integer property with variable
        appendProperty("port.base", "8000");
        appendProperty("port.ref", "${port.base}");
        endConfig();
        
        // ACT: Load configuration
        Path configPath = getConfigPath();
        conf.addResource(configPath);
        
        // ASSERT: Test simple variable substitution - production API
        assertEquals("/user/hadoop/data", conf.get("data.dir"),
            "Simple variable substitution should work");
        
        // Test chained variable substitution
        assertEquals("/user/hadoop/data/output", conf.get("output.dir"),
            "Chained variable substitution should work");
        
        // Test getRaw returns unexpanded value - production API
        assertEquals("${base.dir}/data", conf.getRaw("data.dir"),
            "getRaw should return unexpanded value");
        assertEquals("${data.dir}/output", conf.getRaw("output.dir"),
            "getRaw should return unexpanded value for chained reference");
        
        // Test system property substitution
        String javaInfo = conf.get("java.info");
        assertNotNull(javaInfo, "Java info property should not be null");
        assertTrue(javaInfo.startsWith("Java Version: "),
            "Java info should start with prefix");
        // Verify actual substitution happened (not literal ${java.version})
        assertTrue(!javaInfo.contains("${java.version}"),
            "System property should be substituted");
        
        // Test undefined variable remains as-is
        assertEquals("${undefined.property}/path", conf.get("undefined.ref"),
            "Undefined variable should remain as-is in output");
        
        // Test multiple variable references
        assertEquals("/user/hadoop//user/hadoop", conf.get("multi.ref"),
            "Multiple variable references should all be substituted");
        
        // Test variable substitution with getInt (integer parsing after substitution)
        assertEquals(8000, conf.getInt("port.ref", -1),
            "getInt should work with variable-substituted values");
    }

    /**
     * Tests final property cannot be overridden workflow.
     * 
     * <p>Workflow path: Final property cannot be overridden</p>
     * <p>Production methods invoked:</p>
     * <ul>
     *   <li>{@link Configuration#addResource(Path)} - Load XML configuration file</li>
     *   <li>{@link Configuration#set(String, String)} - Attempt to override final property</li>
     *   <li>{@link Configuration#get(String)} - Retrieve property value</li>
     * </ul>
     * <p>Input conditions:</p>
     * <ul>
     *   <li>XML file with property marked as final</li>
     *   <li>Second XML resource with different value for final property</li>
     *   <li>Programmatic set attempt on final property</li>
     * </ul>
     * <p>Validation criteria:</p>
     * <ul>
     *   <li>Original final value is preserved after override attempts</li>
     *   <li>Second resource with different value is ignored for final properties</li>
     *   <li>Programmatic set does not override final property</li>
     * </ul>
     *
     * @throws IOException if configuration file creation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testFinalProperty() throws IOException {
        // ARRANGE: Create primary XML configuration file with final property
        createConfigFile();
        startConfig();
        // Mark property as final - this should not be overridable
        appendProperty("final.property", "original.final.value", true);
        // Non-final property for comparison
        appendProperty("normal.property", "normal.value", false);
        endConfig();
        
        // ACT: Load primary configuration
        Path configPath = getConfigPath();
        conf.addResource(configPath);
        
        // Force properties to load
        conf.get("final.property");
        
        // ASSERT: Verify initial final value is loaded
        assertEquals("original.final.value", conf.get("final.property"),
            "Final property should be loaded with original value");
        
        // ACT: Attempt programmatic override of final property
        conf.set("final.property", "attempted.override");
        
        // ASSERT: Final property should retain original value
        assertEquals("original.final.value", conf.get("final.property"),
            "Final property should not be overridden by programmatic set");
        
        // ACT: Normal property should allow override
        conf.set("normal.property", "overridden.value");
        assertEquals("overridden.value", conf.get("normal.property"),
            "Non-final property should allow override");
        
        // ARRANGE: Create secondary config file with different value for final property
        File secondConfigFile = createSecondaryConfigFile("override");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(secondConfigFile))) {
            writer.write("<?xml version=\"1.0\"?>\n");
            writer.write("<configuration>\n");
            writer.write("<property>");
            writer.write("<name>final.property</name>");
            writer.write("<value>second.resource.value</value>");
            writer.write("</property>\n");
            writer.write("</configuration>\n");
            writer.flush();
        }
        
        // ACT: Add second resource
        Path secondConfigPath = getConfigPath(secondConfigFile);
        conf.addResource(secondConfigPath);
        
        // Force reload to pick up new resource
        conf.get("final.property");
        
        // ASSERT: Final property should still retain original value
        // (second resource value should be ignored)
        assertEquals("original.final.value", conf.get("final.property"),
            "Final property should not be overridden by second resource");
        
        // Cleanup secondary file
        secondConfigFile.delete();
    }

    /**
     * Tests default values for missing properties workflow.
     * 
     * <p>Workflow path: Default values for missing properties</p>
     * <p>Production methods invoked:</p>
     * <ul>
     *   <li>{@link Configuration#get(String, String)} - Get with default value</li>
     *   <li>{@link Configuration#get(String)} - Get without default</li>
     *   <li>{@link Configuration#getInt(String, int)} - Get integer with default</li>
     *   <li>{@link Configuration#getBoolean(String, boolean)} - Get boolean with default</li>
     * </ul>
     * <p>Input conditions:</p>
     * <ul>
     *   <li>Property names that do not exist in any configuration resource</li>
     *   <li>Various default value types (String, int, boolean)</li>
     * </ul>
     * <p>Validation criteria:</p>
     * <ul>
     *   <li>conf.get("missing", defaultValue) returns default</li>
     *   <li>conf.get("missing") without default returns null</li>
     *   <li>conf.getInt("missing", defaultInt) returns default integer</li>
     *   <li>conf.getBoolean("missing", defaultBool) returns default boolean</li>
     * </ul>
     *
     * @throws IOException if configuration file creation fails
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMissingPropertyDefault() throws IOException {
        // ARRANGE: Create configuration file with some properties (not the ones we'll query)
        createConfigFile();
        startConfig();
        appendProperty("existing.property", "existing.value");
        appendProperty("existing.int", "100");
        appendProperty("existing.bool", "true");
        endConfig();
        
        // ACT: Load configuration
        Path configPath = getConfigPath();
        conf.addResource(configPath);
        
        // Force properties to load
        conf.get("existing.property");
        
        // ASSERT: Test get() without default returns null for missing property - production API
        assertNull(conf.get("nonexistent.property"),
            "get() without default should return null for missing property");
        
        // Test get() with default returns default for missing property - production API
        assertEquals("defaultValue", conf.get("nonexistent.property", "defaultValue"),
            "get() with default should return default for missing property");
        
        // Test getInt() with default returns default for missing property - production API
        assertEquals(42, conf.getInt("nonexistent.int.property", 42),
            "getInt() should return default for missing property");
        
        // Test getBoolean() with default true returns true for missing property - production API
        assertTrue(conf.getBoolean("nonexistent.bool.property", true),
            "getBoolean() should return default true for missing property");
        
        // Test getBoolean() with default false returns false for missing property
        assertEquals(false, conf.getBoolean("nonexistent.bool.property2", false),
            "getBoolean() should return default false for missing property");
        
        // Verify existing properties still work correctly
        assertEquals("existing.value", conf.get("existing.property"),
            "Existing property should return its value, not default");
        assertEquals("existing.value", conf.get("existing.property", "notUsed"),
            "Existing property should return its value, not the default");
        assertEquals(100, conf.getInt("existing.int", -1),
            "Existing int property should return its parsed value");
        assertTrue(conf.getBoolean("existing.bool", false),
            "Existing bool property should return its parsed value");
        
        // Test empty string vs null distinction
        // A property with empty value is different from missing property
        createSecondaryConfig();
        
        // Test getLong with default for missing property
        assertEquals(999L, conf.getLong("nonexistent.long.property", 999L),
            "getLong() should return default for missing property");
        
        // Test getFloat with default for missing property
        assertEquals(3.14f, conf.getFloat("nonexistent.float.property", 3.14f), 0.001f,
            "getFloat() should return default for missing property");
        
        // Test getDouble with default for missing property
        assertEquals(2.718, conf.getDouble("nonexistent.double.property", 2.718), 0.001,
            "getDouble() should return default for missing property");
    }
    
    /**
     * Helper method to create a secondary configuration with empty value for testing.
     * 
     * @throws IOException if file creation fails
     */
    private void createSecondaryConfig() throws IOException {
        File secondConfigFile = createSecondaryConfigFile("empty");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(secondConfigFile))) {
            writer.write("<?xml version=\"1.0\"?>\n");
            writer.write("<configuration>\n");
            writer.write("<property>");
            writer.write("<name>empty.property</name>");
            writer.write("<value></value>");
            writer.write("</property>\n");
            writer.write("</configuration>\n");
            writer.flush();
        }
        
        Path secondConfigPath = getConfigPath(secondConfigFile);
        conf.addResource(secondConfigPath);
        
        // Test that empty string property is different from null (missing)
        // Empty property should return empty string, not null
        String emptyValue = conf.get("empty.property");
        assertEquals("", emptyValue,
            "Property with empty value should return empty string, not null");
        
        // But default should not be used for empty property
        assertEquals("", conf.get("empty.property", "default"),
            "Property with empty value should return empty, not default");
        
        // Cleanup
        secondConfigFile.delete();
    }
}
