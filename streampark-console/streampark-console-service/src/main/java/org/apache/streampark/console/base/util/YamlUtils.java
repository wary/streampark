/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.base.util;

import org.apache.commons.lang3.StringUtils;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class YamlUtils {

    public static void mergeYaml(String yamlStr1, String yamlStr2, Writer output) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> yaml1 = loadYamlFile(yaml, yamlStr1);
        Map<String, Object> yaml2 = loadYamlFile(yaml, yamlStr2);
        Map<String, Object> merged = mergeMaps(yaml1, yaml2);
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        Yaml outputYaml = new Yaml(options);
        outputYaml.dump(merged, output);
    }

    private static Map<String, Object> loadYamlFile(Yaml yaml, String content) throws IOException {
        Map<String, Object> data = null;
        if (StringUtils.isNotBlank(content)) {
            data = yaml.load(content);
        }
        return data != null ? data : new HashMap<>();
    }

    private static Map<String, Object> mergeMaps(Map<String, Object> map1, Map<String, Object> map2) {
        Map<String, Object> result = new LinkedHashMap<>(map1);
        for (Map.Entry<String, Object> entry : map2.entrySet()) {
            String key = entry.getKey();
            Object value2 = entry.getValue();
            if (result.containsKey(key)) {
                Object value1 = result.get(key);
                if (value1 instanceof Map && value2 instanceof Map) {
                    Map<String, Object> mergedNested = mergeMaps(
                        (Map<String, Object>) value1,
                        (Map<String, Object>) value2);
                    result.put(key, mergedNested);
                }
            } else {
                result.put(key, value2);
            }
        }
        return result;
    }
}
