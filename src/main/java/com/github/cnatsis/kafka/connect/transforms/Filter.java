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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Filter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(Filter.class);

    private JsonPath pattern;
    private Action action;

    public static final String OVERVIEW_DOC = "Filter or change a record payload if a pattern is true.";

    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            ConfigName.PATTERN,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            (s, o) -> {
                                try {
                                    JsonPath.compile(String.valueOf(o));
                                } catch (Exception e) {
                                    throw new ConfigException(s, o, e.getMessage());
                                }
                            },
                            ConfigDef.Importance.HIGH,
                            "JsonPath expression (https://github.com/json-path/JsonPath)")
                    .define(
                            ConfigName.ACTION,
                            ConfigDef.Type.STRING,
                            Action.INCLUDE.toString(),
                            (s, o) -> {
                                try {
                                    Action.valueOf(String.valueOf(o).toUpperCase());
                                } catch (Exception e) {
                                    throw new ConfigException(s, o, e.getMessage());
                                }
                            },
                            ConfigDef.Importance.MEDIUM,
                            "Filter actions available are NULLIFY, INCLUDE & EXCLUDE. Defaults to INCLUDE."
                    );

    private interface ConfigName {
        String PATTERN = "pattern";
        String ACTION = "action";
    }

    protected enum Action {
        NULLIFY,
        INCLUDE,
        EXCLUDE;
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        pattern = JsonPath.compile(config.getString(ConfigName.PATTERN));
        action = Action.valueOf(config.getString(ConfigName.ACTION).toUpperCase());
    }

    @Override
    public R apply(R record) {
        try {
            Struct struct = (Struct) record.value();
            if (struct != null) {
                String re = getPayload(struct);
                List<Map<String, Object>> filtered =
                        pattern.read(
                                re,
                                Configuration
                                        .defaultConfiguration()
                                        .addOptions(Option.ALWAYS_RETURN_LIST)
                        );
                if (filtered.isEmpty()) {
                    return null;
                } else {
                    switch (action) {
                        case NULLIFY:
                            return record.newRecord(
                                    record.topic(),
                                    record.kafkaPartition(),
                                    record.keySchema(),
                                    record.key(),
                                    null,
                                    null,
                                    record.timestamp()
                            );
                        case EXCLUDE:
                            return null;
                        default:
                            return record;
                    }
                }
            } else {
                return null;
            }
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage());
            return record;
        }
    }

    private String getPayload(Struct struct) throws JsonProcessingException {
        Map<String, Object> payload = new HashMap<>();
        struct.schema()
                .fields()
                .forEach(field -> {
                    Object input = struct.get(field.name());
                    if (input != null) {
                        payload.put(field.name(), input);
                    }
                });
        ObjectMapper om = new ObjectMapper();
        return om.writeValueAsString(payload);
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}