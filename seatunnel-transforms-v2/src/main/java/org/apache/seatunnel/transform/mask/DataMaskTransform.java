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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.MaskColumn;
import org.apache.seatunnel.transform.mask.masker.FieldMasker;
import org.apache.seatunnel.transform.mask.masker.MaskType;
import org.apache.seatunnel.transform.mask.masker.NormalizeMasker;
import org.apache.seatunnel.transform.mask.masker.OffsetMasker;
import org.apache.seatunnel.transform.mask.masker.ReplaceMasker;
import org.apache.seatunnel.transform.mask.masker.TruncateMasker;

import lombok.NonNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 数据脱敏 transform：按列配置脱敏类型与明细。
 *
 * <p>输出类型规则：TRUNCATE/OFFSET/REPLACE 输出列覆写为 STRING；
 * NORMALIZE 所有 target 可解析为原列类型时保持原类型，否则覆写 STRING。
 *
 * <p>约束（初始化阶段 fail-fast）：字段必须存在；TRUNCATE/OFFSET/REPLACE 仅支持标量类型列；
 * NORMALIZE 仅支持数值族列；明细结构非法直接抛错。
 */
public class DataMaskTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "DataMask";

    /** 字符类脱敏(TRUNCATE/OFFSET/REPLACE)支持的标量类型白名单 */
    private static final Set<SqlType> SCALAR_TYPES =
            EnumSet.of(
                    SqlType.BOOLEAN,
                    SqlType.STRING,
                    SqlType.TINYINT,
                    SqlType.SMALLINT,
                    SqlType.INT,
                    SqlType.BIGINT,
                    SqlType.FLOAT,
                    SqlType.DOUBLE,
                    SqlType.DECIMAL,
                    SqlType.DATE,
                    SqlType.TIME,
                    SqlType.TIMESTAMP);

    /** NORMALIZE 支持的数值族类型 */
    private static final Set<SqlType> NUMERIC_TYPES =
            EnumSet.of(
                    SqlType.TINYINT,
                    SqlType.SMALLINT,
                    SqlType.INT,
                    SqlType.BIGINT,
                    SqlType.FLOAT,
                    SqlType.DOUBLE,
                    SqlType.DECIMAL);

    private final List<MaskColumn> maskColumns;
    private final int[] inputFieldIndexes;
    private final FieldMasker[] maskers;
    /** 各列的输出类型（STRING 或保持原类型） */
    private final SeaTunnelDataType<?>[] outputTypes;

    public DataMaskTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.maskColumns = config.get(DataMaskTransformConfig.MASK_COLUMNS);
        if (maskColumns == null || maskColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    PLUGIN_NAME + " transform 至少需要配置一个脱敏列(mask_columns)");
        }

        SeaTunnelRowType rowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        int size = maskColumns.size();
        this.inputFieldIndexes = new int[size];
        this.maskers = new FieldMasker[size];
        this.outputTypes = new SeaTunnelDataType<?>[size];
        for (int i = 0; i < size; i++) {
            MaskColumn column = maskColumns.get(i);
            // 定位字段, 不存在直接报错
            try {
                inputFieldIndexes[i] = rowType.indexOf(column.getField());
            } catch (IllegalArgumentException e) {
                throw TransformCommonError.cannotFindInputFieldError(
                        getPluginName(), column.getField());
            }
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(inputFieldIndexes[i]);
            MaskType maskType = MaskType.of(column.getMaskType());
            // 按类型做列类型约束校验并构建脱敏器
            switch (maskType) {
                case TRUNCATE:
                    requireScalarType(fieldType, column.getField());
                    maskers[i] = new TruncateMasker(column.getTruncateRules());
                    outputTypes[i] = BasicType.STRING_TYPE;
                    break;
                case OFFSET:
                    requireScalarType(fieldType, column.getField());
                    maskers[i] =
                            new OffsetMasker(
                                    column.getOffsetDirection(), column.getOffsetLength());
                    outputTypes[i] = BasicType.STRING_TYPE;
                    break;
                case REPLACE:
                    requireScalarType(fieldType, column.getField());
                    maskers[i] = new ReplaceMasker(column.getReplaceRules());
                    outputTypes[i] = BasicType.STRING_TYPE;
                    break;
                case NORMALIZE:
                default:
                    if (!NUMERIC_TYPES.contains(fieldType.getSqlType())) {
                        throw CommonError.unsupportedDataType(
                                getPluginName(),
                                fieldType.getSqlType().toString(),
                                column.getField());
                    }
                    NormalizeMasker normalizeMasker =
                            new NormalizeMasker(column.getNormalizeRules(), fieldType);
                    maskers[i] = normalizeMasker;
                    outputTypes[i] =
                            normalizeMasker.isKeepOriginalType()
                                    ? fieldType
                                    : BasicType.STRING_TYPE;
                    break;
            }
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Column[] getOutputColumns() {
        Map<String, Column> columnsByName =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        Column[] outputColumns = new Column[maskColumns.size()];
        for (int i = 0; i < maskColumns.size(); i++) {
            Column source = columnsByName.get(maskColumns.get(i).getField());
            // 同名列 => 基类按同名覆写值; 类型变化时一并覆写列类型
            outputColumns[i] = source.copy(outputTypes[i]);
        }
        return outputColumns;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] values = new Object[inputFieldIndexes.length];
        for (int i = 0; i < inputFieldIndexes.length; i++) {
            Object value = inputRow.getField(inputFieldIndexes[i]);
            // null 直通
            values[i] = value == null ? null : maskers[i].mask(value);
        }
        return values;
    }

    private void requireScalarType(SeaTunnelDataType<?> fieldType, String field) {
        if (!SCALAR_TYPES.contains(fieldType.getSqlType())) {
            throw CommonError.unsupportedDataType(
                    getPluginName(), fieldType.getSqlType().toString(), field);
        }
    }
}
