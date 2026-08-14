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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.encrypt.EncryptTransformConfig.EncryptColumn;
import org.apache.seatunnel.transform.encrypt.encryptor.EncryptAlgorithm;
import org.apache.seatunnel.transform.encrypt.encryptor.FieldEncryptor;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 字段加密 transform：按列选择算法加密，输出列类型一律覆写为 STRING。
 *
 * <p>约束（初始化阶段 fail-fast）：
 * <ul>
 *   <li>字段必须存在于输入 schema；</li>
 *   <li>仅支持标量类型列（BYTES/ARRAY/MAP/ROW 的 String.valueOf 产出无意义值，禁配）；</li>
 *   <li>密钥长度按算法校验（AES 16/24/32、DES 8、SM4 16 字节）。</li>
 * </ul>
 */
public class EncryptTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "Encrypt";

    /** 允许加密的标量类型白名单（日期时间类按 ISO 格式 toString 后加密） */
    private static final Set<SqlType> SUPPORTED_TYPES =
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

    private final List<EncryptColumn> encryptColumns;
    private final int[] inputFieldIndexes;
    private final FieldEncryptor[] encryptors;

    public EncryptTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.encryptColumns = config.get(EncryptTransformConfig.ENCRYPT_COLUMNS);
        if (encryptColumns == null || encryptColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    PLUGIN_NAME + " transform 至少需要配置一个加密列(encrypt_columns)");
        }

        SeaTunnelRowType rowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        this.inputFieldIndexes = new int[encryptColumns.size()];
        this.encryptors = new FieldEncryptor[encryptColumns.size()];
        for (int i = 0; i < encryptColumns.size(); i++) {
            EncryptColumn column = encryptColumns.get(i);
            // 定位字段, 不存在直接报错
            try {
                inputFieldIndexes[i] = rowType.indexOf(column.getField());
            } catch (IllegalArgumentException e) {
                throw TransformCommonError.cannotFindInputFieldError(
                        getPluginName(), column.getField());
            }
            // 仅支持标量类型
            SqlType sqlType = rowType.getFieldType(inputFieldIndexes[i]).getSqlType();
            if (!SUPPORTED_TYPES.contains(sqlType)) {
                throw CommonError.unsupportedDataType(
                        getPluginName(), sqlType.toString(), column.getField());
            }
            // 创建加密器, 内含算法与密钥合法性校验
            encryptors[i] =
                    EncryptAlgorithm.of(column.getAlgorithm())
                            .createEncryptor(column.getSecretKey(), column.getSalt());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    /**
     * 覆写父类逻辑：先浅拷贝输入行再交给父类处理。
     *
     * <p>引擎在"一个上游分叉给多个下游"时向所有下游传递同一个 SeaTunnelRow 引用
     * （见 SeaTunnelSourceCollector#sendRecordToNext）；而父类在不新增列时走 REUSE_ROW
     * 原地修改输入行，会污染其他链路（如另一条链配置了脱敏，两个 sink 会拿到同样的值）。
     * 先 copy 保证本 transform 只修改私有副本，与拓扑无关地保持正确性。
     */
    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return super.transformRow(inputRow.copy());
    }

    @Override
    protected Column[] getOutputColumns() {
        Map<String, Column> columnsByName =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        Column[] outputColumns = new Column[encryptColumns.size()];
        for (int i = 0; i < encryptColumns.size(); i++) {
            Column source = columnsByName.get(encryptColumns.get(i).getField());
            // 同名列 + STRING 类型 => 基类按同名覆写值与列类型
            outputColumns[i] = source.copy(BasicType.STRING_TYPE);
        }
        return outputColumns;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] values = new Object[inputFieldIndexes.length];
        for (int i = 0; i < inputFieldIndexes.length; i++) {
            Object value = inputRow.getField(inputFieldIndexes[i]);
            // null 直通; 非字符串标量先 String.valueOf(日期时间类即 ISO 格式)
            values[i] = value == null ? null : encryptors[i].encrypt(String.valueOf(value));
        }
        return values;
    }
}
