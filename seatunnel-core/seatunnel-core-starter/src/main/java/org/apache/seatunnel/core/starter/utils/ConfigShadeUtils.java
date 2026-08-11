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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.configuration.ConfigShade;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiFunction;

/** Config shade utilities */
@Slf4j
public final class ConfigShadeUtils {

    private static final String SHADE_IDENTIFIER_OPTION = "shade.identifier";
    private static final String SHADE_PROPS_OPTION = "shade.properties";
    private static final String SHADE_OPTIONS_OPTION = "shade.options";

    public static final String[] DEFAULT_SENSITIVE_KEYWORDS =
            new String[] {"password", "username", "auth", "token", "access_key", "secret_key"};

    /**
     * 局部密文标记前缀，完整形态为 ENC(密文)。
     *
     * <p>解密时，任意嵌套层级的字符串值只要包含该前缀，就会被整体交给 {@link ConfigShade#decrypt}，
     * 由具体实现负责识别标记、替换其中的密文片段。
     */
    public static final String ENCRYPTED_MARKER_PREFIX = "ENC(";

    /**
     * 局部明文标记前缀，完整形态为 DEC(明文)。
     *
     * <p>加密时的对应标记，语义与 {@link #ENCRYPTED_MARKER_PREFIX} 相同，方向相反。
     */
    public static final String DECRYPTED_MARKER_PREFIX = "DEC(";

    private static final Map<String, ConfigShade> CONFIG_SHADES = new HashMap<>();

    private static final ConfigShade DEFAULT_SHADE = new DefaultConfigShade();

    static {
        ServiceLoader<ConfigShade> serviceLoader = ServiceLoader.load(ConfigShade.class);
        Iterator<ConfigShade> it = serviceLoader.iterator();
        it.forEachRemaining(
                configShade -> {
                    CONFIG_SHADES.put(configShade.getIdentifier(), configShade);
                });
        log.info("Load config shade spi: {}", CONFIG_SHADES.keySet());
    }

    private static class DefaultConfigShade implements ConfigShade {
        private static final String IDENTIFIER = "default";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return content;
        }

        @Override
        public String decrypt(String content) {
            return content;
        }
    }

    public static String encryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.encrypt(content);
    }

    public static String decryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.decrypt(content);
    }

    public static Config decryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        Map<String, Object> props =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_PROPS_OPTION,
                        new HashMap<>());
        return decryptConfig(identifier, config, props);
    }

    public static Config encryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        Map<String, Object> props =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_PROPS_OPTION,
                        new HashMap<>());
        return encryptConfig(identifier, config, props);
    }

    private static Config decryptConfig(
            String identifier, Config config, Map<String, Object> props) {
        return processConfig(identifier, config, true, props);
    }

    private static Config encryptConfig(
            String identifier, Config config, Map<String, Object> props) {
        return processConfig(identifier, config, false, props);
    }

    @SuppressWarnings("unchecked")
    private static Config processConfig(
            String identifier, Config config, boolean isDecrypted, Map<String, Object> props) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        // call open method before the encrypt/decrypt
        configShade.open(props);

        Set<String> sensitiveOptions = new HashSet<>(getSensitiveOptions(config));
        sensitiveOptions.addAll(Arrays.asList(configShade.sensitiveOptions()));
        BiFunction<String, Object, Object> processFunction =
                (key, value) -> {
                    if (value instanceof List) {
                        List<String> list = (List<String>) value;
                        List<String> processedList = new ArrayList<>();
                        for (String element : list) {
                            processedList.add(
                                    isDecrypted
                                            ? configShade.decrypt(element)
                                            : configShade.encrypt(element));
                        }
                        return processedList;
                    } else {
                        return isDecrypted
                                ? configShade.decrypt((String) value)
                                : configShade.encrypt((String) value);
                    }
                };
        String jsonString = config.root().render(ConfigRenderOptions.concise());
        ObjectNode jsonNodes = JsonUtils.parseObject(jsonString);
        Map<String, Object> configMap = JsonUtils.toMap(jsonNodes);
        List<Map<String, Object>> sources =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SOURCE);
        List<Map<String, Object>> sinks =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SINK);
        List<Map<String, Object>> transforms =
                (ArrayList<Map<String, Object>>)
                        configMap.getOrDefault(Constants.TRANSFORM, new ArrayList<>());
        Preconditions.checkArgument(
                !sources.isEmpty(), "Miss <Source> config! Please check the config file.");
        Preconditions.checkArgument(
                !sinks.isEmpty(), "Miss <Sink> config! Please check the config file.");
        sources.forEach(
                source -> {
                    for (String sensitiveOption : sensitiveOptions) {
                        source.computeIfPresent(sensitiveOption, processFunction);
                    }
                });
        sinks.forEach(
                sink -> {
                    for (String sensitiveOption : sensitiveOptions) {
                        sink.computeIfPresent(sensitiveOption, processFunction);
                    }
                });
        transforms.forEach(
                transform -> {
                    for (String sensitiveOption : sensitiveOptions) {
                        transform.computeIfPresent(sensitiveOption, processFunction);
                    }
                });
        configMap.put(Constants.SOURCE, sources);
        configMap.put(Constants.SINK, sinks);
        configMap.put(Constants.TRANSFORM, transforms);

        // 顶层敏感字段处理完毕后，再按 ENC()/DEC() 标记递归处理任意嵌套层级。
        //
        // 两步的先后顺序不可颠倒：顶层敏感字段在上一步已经被整体加解密，其结果不再含有标记，
        // 递归时会自然跳过，因此同一个值不会被处理两次。
        processMarkedValues(configMap, isDecrypted, configShade);

        return ConfigFactory.parseMap(configMap);
    }

    /**
     * 递归遍历配置树，对带有 ENC()/DEC() 标记的字符串值执行加解密。
     *
     * <p>与顶层的敏感字段匹配不同，这里完全不看 key 叫什么，只认值里的显式标记。
     * 这是为了覆盖 Kafka 的 {@code sasl.jaas.config} 这类场景——敏感信息埋在
     * 嵌套 map 的一整串文本中间，其 key（{@code sasl.jaas.config}）既不是 password，
     * 也不可能穷举进敏感字段列表。
     *
     * <p>反过来，「只认标记」也保证了不会误伤：{@code schema.fields} 下恰好名为
     * password 的业务字段没有标记，因此不会被当成密文处理。
     *
     * @param node 当前遍历到的节点，可能是 Map、List 或标量
     * @param isDecrypted true 表示解密，false 表示加密
     * @param configShade 实际执行加解密的实现
     */
    @SuppressWarnings("unchecked")
    private static void processMarkedValues(
            Object node, boolean isDecrypted, ConfigShade configShade) {
        if (node instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) node).entrySet()) {
                Object original = entry.getValue();
                Object processed = processMarkedValue(original, isDecrypted, configShade);
                // 仅在值确实发生变化时才写回，避免对不可变集合做无谓的写操作
                if (processed != original) {
                    entry.setValue(processed);
                }
            }
        } else if (node instanceof List) {
            List<Object> list = (List<Object>) node;
            for (int i = 0; i < list.size(); i++) {
                Object original = list.get(i);
                Object processed = processMarkedValue(original, isDecrypted, configShade);
                if (processed != original) {
                    list.set(i, processed);
                }
            }
        }
    }

    /**
     * 处理单个节点值：字符串按标记加解密，容器类型继续下钻。
     *
     * @return 处理后的值；未命中标记时原样返回入参对象（调用方据此判断是否需要写回）
     */
    private static Object processMarkedValue(
            Object value, boolean isDecrypted, ConfigShade configShade) {
        // 只有字符串才可能携带标记，其余类型继续向下递归
        if (!(value instanceof String)) {
            processMarkedValues(value, isDecrypted, configShade);
            return value;
        }
        String text = (String) value;
        String marker = isDecrypted ? ENCRYPTED_MARKER_PREFIX : DECRYPTED_MARKER_PREFIX;
        // 不含标记的值一律不碰，确保存量配置的行为完全不变
        if (!text.contains(marker)) {
            return value;
        }
        return isDecrypted ? configShade.decrypt(text) : configShade.encrypt(text);
    }

    public static Set<String> getSensitiveOptions(Config config) {
        Set<String> sensitiveOptions =
                new HashSet<>(
                        TypesafeConfigUtils.getConfig(
                                config != null && config.hasPath(Constants.ENV)
                                        ? config.getConfig(Constants.ENV)
                                        : ConfigFactory.empty(),
                                SHADE_OPTIONS_OPTION,
                                new ArrayList<>()));
        sensitiveOptions.addAll(Arrays.asList(DEFAULT_SENSITIVE_KEYWORDS));
        return sensitiveOptions;
    }

    public static class Base64ConfigShade implements ConfigShade {

        private static final Base64.Encoder ENCODER = Base64.getEncoder();

        private static final Base64.Decoder DECODER = Base64.getDecoder();

        private static final String IDENTIFIER = "base64";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return ENCODER.encodeToString(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String decrypt(String content) {
            return new String(DECODER.decode(content));
        }
    }
}
