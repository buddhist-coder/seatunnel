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

package org.apache.seatunnel.transform.mask.masker;

import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.ReplaceRule;

import java.util.ArrayList;
import java.util.List;

/**
 * 替换脱敏器：将子串的所有出现替换为目标内容，多条规则按配置顺序**链式**应用
 * （后一条作用于前一条的结果）。字面量匹配，非正则。输出恒为 String。
 */
public class ReplaceMasker implements FieldMasker {

    private static final long serialVersionUID = 1L;

    private final List<Entry> entries;

    public ReplaceMasker(List<ReplaceRule> rules) {
        if (rules == null || rules.isEmpty()) {
            throw new IllegalArgumentException("REPLACE 脱敏至少需要一条替换规则(replace_rules)");
        }
        this.entries = new ArrayList<>(rules.size());
        for (ReplaceRule rule : rules) {
            if (rule.getSearch() == null || rule.getSearch().isEmpty()) {
                throw new IllegalArgumentException("REPLACE 被替换内容(search)不能为空");
            }
            entries.add(
                    new Entry(
                            rule.getSearch(),
                            rule.getReplacement() == null ? "" : rule.getReplacement()));
        }
    }

    @Override
    public Object mask(Object value) {
        String result = String.valueOf(value);
        for (Entry entry : entries) {
            result = result.replace(entry.search, entry.replacement);
        }
        return result;
    }

    /** 单条替换规则 */
    private static class Entry implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final String search;
        private final String replacement;

        Entry(String search, String replacement) {
            this.search = search;
            this.replacement = replacement;
        }
    }
}
