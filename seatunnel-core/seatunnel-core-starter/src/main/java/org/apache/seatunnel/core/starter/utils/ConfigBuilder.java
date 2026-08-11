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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.impl.Parseable;

import org.apache.seatunnel.api.configuration.ConfigAdapter;
import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.ParserException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.utils.PlaceholderUtils.replacePlaceholders;

/** Used to build the {@link Config} from config file. */
@Slf4j
public class ConfigBuilder {

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS =
            ConfigRenderOptions.concise().setFormatted(true);

    private static final String PLACEHOLDER_REGEX = "\\$\\{([^:{}]+)(?::[^}]*)?\\}";

    /**
     * 内嵌敏感片段的匹配模式，用于日志脱敏。
     *
     * <p>{@link #configDesensitization} 原本只按 key 判断敏感字段，但有些敏感信息是嵌在
     * 一整串文本值的中间的，其 key 本身毫无敏感特征，例如 Kafka 的：
     *
     * <pre>
     * "sasl.jaas.config" = "...PlainLoginModule required username=\"admin\" password=\"真实密码\";"
     * </pre>
     *
     * <p>这类值在配置解密后就是明文，若原样打印会把密码泄漏进日志文件，因此这里按
     * {@code 关键字=值} 的形态额外做一次脱敏。同样能覆盖 JDBC URL 上的 {@code ?password=xxx}
     * 以及 SQL 语句里的密码字面量。
     */
    private static final Pattern EMBEDDED_SENSITIVE_PATTERN =
            Pattern.compile(
                    "(?i)(password|passwd|secret|token|access_key|secret_key)(\\s*=\\s*)"
                            + "(\"[^\"]*\"|'[^']*'|[^\\s;,&]+)");

    private ConfigBuilder() {
        // utility class and cannot be instantiated
    }

    private static Config ofInner(@NonNull Path filePath, List<String> variables) {
        Config config =
                ConfigFactory.parseFile(filePath.toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
    }

    public static Config of(@NonNull String filePath) {
        Path path = Paths.get(filePath);
        return of(path);
    }

    public static Config of(@NonNull String filePath, List<String> variables) {
        Path path = Paths.get(filePath);
        return of(path, variables);
    }

    public static Config of(@NonNull Path filePath) {
        return of(filePath, null);
    }

    public static Config of(@NonNull Path filePath, List<String> variables) {
        log.info("Loading config file from path: {}", filePath);
        Optional<ConfigAdapter> adapterSupplier = ConfigAdapterUtils.selectAdapter(filePath);
        Config config =
                adapterSupplier
                        .map(adapter -> of(adapter, filePath, variables))
                        .orElseGet(() -> ofInner(filePath, variables));
        log.info(
                "Parsed config file: \n{}",
                mapToString(
                        configDesensitization(
                                config.root().unwrapped(),
                                ConfigShadeUtils.getSensitiveOptions(config))));
        return config;
    }

    public static Config of(@NonNull Map<String, Object> objectMap) {
        return of(objectMap, false);
    }

    public static Config of(@NonNull Map<String, Object> objectMap, boolean isEncrypt) {
        log.info("Loading config file from objectMap");
        Config config =
                ConfigFactory.parseMap(objectMap)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        if (!isEncrypt) {
            config = ConfigShadeUtils.decryptConfig(config);
        }
        log.info(
                "Parsed config file: \n{}",
                mapToString(
                        configDesensitization(
                                config.root().unwrapped(),
                                ConfigShadeUtils.getSensitiveOptions(config))));
        return config;
    }

    public static Map<String, Object> configDesensitization(
            Map<String, Object> configMap, Set<String> sensitiveKeywords) {
        return configMap.entrySet().stream()
                .collect(
                        LinkedHashMap::new,
                        (m, p) -> {
                            String key = p.getKey();
                            Object value = p.getValue();
                            if (sensitiveKeywords.contains(key.toLowerCase())) {
                                if (value instanceof List<?>) {
                                    List<Object> maskedList =
                                            ((List<?>) value)
                                                    .stream()
                                                            .map(v -> "******")
                                                            .collect(Collectors.toList());
                                    m.put(key, maskedList);
                                } else {
                                    m.put(key, "******");
                                }
                            } else {
                                if (value instanceof Map<?, ?>) {
                                    m.put(
                                            key,
                                            configDesensitization(
                                                    (Map<String, Object>) value,
                                                    sensitiveKeywords));
                                } else if (value instanceof List<?>) {
                                    List<?> listValue = (List<?>) value;
                                    List<Object> newList =
                                            listValue.stream()
                                                    .map(
                                                            v -> {
                                                                if (v instanceof Map<?, ?>) {
                                                                    return configDesensitization(
                                                                            (Map<String, Object>) v,
                                                                            sensitiveKeywords);
                                                                } else if (v instanceof String) {
                                                                    return desensitizeEmbeddedSecrets(
                                                                            (String) v);
                                                                } else {
                                                                    return v;
                                                                }
                                                            })
                                                    .collect(Collectors.toList());
                                    m.put(key, newList);
                                } else if (value instanceof String) {
                                    // key 本身不敏感，但值里可能嵌着 password=xxx 之类的片段
                                    m.put(key, desensitizeEmbeddedSecrets((String) value));
                                } else {
                                    m.put(key, value);
                                }
                            }
                        },
                        LinkedHashMap::putAll);
    }

    /**
     * 对字符串值内部嵌套的敏感片段做脱敏，仅用于日志输出。
     *
     * <p>只替换 {@code 关键字=值} 中的值部分，关键字、分隔符和引号形态都原样保留，
     * 这样日志仍能与原始配置对照，便于排查问题。
     *
     * @param value 原始字符串值
     * @return 脱敏后的字符串；未命中任何敏感片段时原样返回
     */
    private static String desensitizeEmbeddedSecrets(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        Matcher matcher = EMBEDDED_SENSITIVE_PATTERN.matcher(value);
        if (!matcher.find()) {
            return value;
        }
        matcher.reset();
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String rawValue = matcher.group(3);
            // 保留原有的引号形态，只把引号内部的内容抹掉
            String quote =
                    rawValue.length() >= 2 && (rawValue.startsWith("\"") || rawValue.startsWith("'"))
                            ? rawValue.substring(0, 1)
                            : "";
            matcher.appendReplacement(
                    buffer,
                    Matcher.quoteReplacement(
                            matcher.group(1) + matcher.group(2) + quote + "******" + quote));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    public static Config of(
            @NonNull ConfigAdapter configAdapter, @NonNull Path filePath, List<String> variables) {
        log.info("With config adapter spi {}", configAdapter.getClass().getName());
        try {
            Map<String, Object> flattenedMap = configAdapter.loadConfig(filePath);
            Config config = ConfigFactory.parseMap(flattenedMap);
            return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
        } catch (ParserException | IllegalArgumentException e) {
            throw e;
        } catch (Exception warn) {
            log.warn(
                    "Loading config failed with spi {}, fallback to HOCON loader.",
                    configAdapter.getClass().getName());
            return ofInner(filePath, variables);
        }
    }

    private static Config backfillUserVariables(Config config, List<String> variables) {
        if (variables != null) {
            variables.stream()
                    .filter(Objects::nonNull)
                    .map(variable -> variable.split("=", 2))
                    .filter(pair -> pair.length == 2)
                    .peek(
                            pair -> {
                                if (TablePlaceholder.isSystemPlaceholder(pair[0])) {
                                    throw new ConfigCheckException(
                                            "System placeholders cannot be used. Incorrect config parameter: "
                                                    + pair[0]);
                                }
                            })
                    .forEach(pair -> System.setProperty(pair[0], pair[1]));
            Config systemConfig =
                    Parseable.newProperties(
                                    System.getProperties(),
                                    ConfigParseOptions.defaults()
                                            .setOriginDescription("system properties"))
                            .parse()
                            .toConfig();

            Config resolvedConfig =
                    config.resolveWith(
                            systemConfig, ConfigResolveOptions.defaults().setAllowUnresolved(true));

            Map<String, Object> configMap = resolvedConfig.root().unwrapped();

            configMap.forEach(
                    (key, value) -> {
                        if (value instanceof Map) {
                            processVariablesMap((Map<String, Object>) value);
                        } else if (value instanceof List) {
                            ((List<Map<String, Object>>) value)
                                    .forEach(map -> processVariablesMap(map));
                        }
                    });

            return ConfigFactory.parseString(
                            JsonUtils.toJsonString(configMap),
                            ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        }
        return config;
    }

    private static void processVariablesMap(Map<String, Object> mapValue) {
        mapValue.forEach(
                (innerKey, innerValue) -> {
                    if (innerValue instanceof Map) {
                        processVariablesMap((Map<String, Object>) innerValue);
                    } else if (innerValue instanceof List) {
                        mapValue.put(innerKey, processVariablesList((List<?>) innerValue));
                    } else {
                        processVariable(innerKey, innerValue, mapValue);
                    }
                });
    }

    private static List<?> processVariablesList(List<?> list) {
        return list.stream()
                .map(
                        variable -> {
                            if (variable instanceof String) {
                                String variableString = (String) variable;
                                return extractPlaceholder(variableString).stream()
                                        .reduce(
                                                variableString,
                                                (result, placeholder) -> {
                                                    return replacePlaceholders(
                                                            result,
                                                            placeholder,
                                                            System.getProperty(placeholder),
                                                            null);
                                                });
                            } else if (variable instanceof Map) {
                                processVariablesMap((Map<String, Object>) variable);
                                return variable;
                            } else if (variable instanceof List) {
                                return processVariablesList((List<?>) variable);
                            }
                            return variable;
                        })
                .collect(Collectors.toList());
    }

    private static void processVariable(
            String variableKey, Object variableValue, Map<String, Object> parentMap) {
        if (Objects.isNull(variableValue)) {
            return;
        }
        String variableString = variableValue.toString();
        List<String> placeholders = extractPlaceholder(variableString);

        for (String placeholder : placeholders) {
            String replacedValue =
                    replacePlaceholders(
                            variableString, placeholder, System.getProperty(placeholder), null);
            variableString = replacedValue;
        }

        if (!placeholders.isEmpty()) {
            parentMap.put(variableKey, variableString);
        }
    }

    public static List<String> extractPlaceholder(String input) {
        Pattern pattern = Pattern.compile(PLACEHOLDER_REGEX);
        Matcher matcher = pattern.matcher(input);
        List<String> placeholders = new ArrayList<>();

        while (matcher.find()) {
            placeholders.add(matcher.group(1));
        }

        return placeholders;
    }

    public static String mapToString(Map<String, Object> configMap) {
        ConfigParseOptions configParseOptions =
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON);
        Config config =
                ConfigFactory.parseString(JsonUtils.toJsonString(configMap), configParseOptions)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return config.root().render(CONFIG_RENDER_OPTIONS);
    }
}
