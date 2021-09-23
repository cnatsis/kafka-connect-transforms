/*
 * Copyright © 2021 Christos Natsis
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.cnatsis.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FilterTest {

    private final Filter<SourceRecord> fObj = new Filter<>();

    @Test
    public void testSchemaless() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "[?(@.myField == \"test\")]");
        props.put("action", "nullify");

        final SourceRecord filteredRecord = fObj.apply(createRecord(props, false));

        assertNull(filteredRecord);
    }

    @Test
    public void testEmptyFilterResult() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "[?(@.myField == \"wrong\")]");
        props.put("action", "nullify");
        final SourceRecord filteredRecord = fObj.apply(createRecord(props, true));
        assertNull(filteredRecord);
    }

    @Test
    public void testWithSchemaNullify() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "[?(@.myField == \"test\")]");
        props.put("action", "nullify");

        final SourceRecord filteredRecord = fObj.apply(createRecord(props, true));

        assertNull(filteredRecord.valueSchema());
        assertNull(filteredRecord.value());
    }

    @Test
    public void testWithSchemaExclude() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "[?(@.myField == \"test\")]");
        props.put("action", "exclude");

        final SourceRecord filteredRecord = fObj.apply(createRecord(props, true));

        assertNull(filteredRecord);
    }

    @Test
    public void testWithSchemaInclude() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "[?(@.myField == \"test\")]");

        final SourceRecord filteredRecord = fObj.apply(createRecord(props, true));

        assertNotNull(filteredRecord.valueSchema());
        assertNotNull(filteredRecord.value());
    }

    @Test
    public void testInvalidPattern() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "?(@.myField == \"test\")]");
        props.put("action", "nullify");
        ConfigException e = assertThrows(ConfigException.class, () -> fObj.configure(props));
        assertEquals(
                "Invalid value ?(@.myField == \"test\")] for configuration pattern: Use bracket notion ['my prop'] if your property contains blank characters. position: 2",
                e.getMessage());
    }

    @Test
    public void testInvalidAction() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("pattern", "[?(@.myField == \"test\")]");
        props.put("action", "faulty");
        ConfigException e = assertThrows(ConfigException.class, () -> fObj.configure(props));
        assertTrue(e.getMessage().contains("No enum constant com.github.cnatsis.kafka.connect.transforms.Filter.Action.FAULTY"));
    }

    private SourceRecord createRecord(Map<String, ?> props, boolean hasSchema) {
        fObj.configure(props);
        Schema valueSchema = null;
        Struct valuePayload = null;

        if (hasSchema) {
            valueSchema =
                    SchemaBuilder
                            .struct()
                            .field("myField", Schema.STRING_SCHEMA)
                            .build();
            valuePayload =
                    new Struct(valueSchema)
                            .put("myField", "test");
        }

        return new SourceRecord(
                null,
                null,
                "test",
                null,
                null,
                valueSchema,
                valuePayload);
    }

}