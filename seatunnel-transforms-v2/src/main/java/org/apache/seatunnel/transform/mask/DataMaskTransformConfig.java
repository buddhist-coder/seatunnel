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

package org.apache.seatunnel.transform.mask;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * DataMask transform 的配置定义。
 *
 * <p>配置示例：
 *
 * <pre>{@code
 * DataMask {
 *   plugin_input  = "..."
 *   plugin_output = "..."
 *   mask_columns = [
 *     { field = "phone", mask_type = "TRUNCATE",
 *       truncate_rules = [ { position = "FRONT", length = 3 } ] },
 *     { field = "account", mask_type = "OFFSET",
 *       offset_direction = "FORWARD", offset_length = 2 },
 *     { field = "age", mask_type = "NORMALIZE",
 *       normalize_rules = [ { operator = "LT", compare = 30, target = "20" } ] },
 *     { field = "email", mask_type = "REPLACE",
 *       replace_rules = [ { search = "@qq.com", replacement = "@***.com" } ] }
 *   ]
 * }
 * }</pre>
 */
public class DataMaskTransformConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Option<List<MaskColumn>> MASK_COLUMNS =
            Options.key("mask_columns")
                    .listType(MaskColumn.class)
                    .noDefaultValue()
                    .withDescription("The columns to mask, each with field/mask_type and details");

    /** 单列脱敏配置，按 mask_type 取用对应明细 */
    @Data
    public static class MaskColumn implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 字段名（须存在于输入 schema） */
        @JsonAlias("field")
        private String field;

        /** 脱敏类型: TRUNCATE/OFFSET/NORMALIZE/REPLACE */
        @JsonAlias("mask_type")
        private String maskType;

        /** 截断明细（TRUNCATE 必填，多段有序） */
        @JsonAlias("truncate_rules")
        private List<TruncateRule> truncateRules;

        /** 偏移方向: FORWARD/BACKWARD（OFFSET 必填） */
        @JsonAlias("offset_direction")
        private String offsetDirection;

        /** 偏移位数（OFFSET 必填，> 0） */
        @JsonAlias("offset_length")
        private Integer offsetLength;

        /** 规整明细（NORMALIZE 必填，多行有序，首个命中生效） */
        @JsonAlias("normalize_rules")
        private List<NormalizeRule> normalizeRules;

        /** 替换明细（REPLACE 必填，多条链式） */
        @JsonAlias("replace_rules")
        private List<ReplaceRule> replaceRules;
    }

    /** 截断单段：删除 [前/后/中间] N 位 */
    @Data
    public static class TruncateRule implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 位置: FRONT/BACK/MIDDLE */
        @JsonAlias("position")
        private String position;

        /** 删除位数，> 0 */
        @JsonAlias("length")
        private Integer length;
    }

    /** 规整单行：[运算符] 比较值 属于 目标值 */
    @Data
    public static class NormalizeRule implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 运算符: GT/GE/EQ/LT/LE */
        @JsonAlias("operator")
        private String operator;

        /** 比较值（必须可解析为数值） */
        @JsonAlias("compare")
        private String compare;

        /** 目标值（命中后的输出） */
        @JsonAlias("target")
        private String target;
    }

    /** 替换单条：将 search 的所有出现替换为 replacement */
    @Data
    public static class ReplaceRule implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 被替换内容（字面量子串，非正则，非空） */
        @JsonAlias("search")
        private String search;

        /** 替换为（可为空串 = 删除） */
        @JsonAlias("replacement")
        private String replacement;
    }
}
