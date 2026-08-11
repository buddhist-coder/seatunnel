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

import java.io.Serializable;

/**
 * 字段脱敏器策略接口。
 *
 * <p>实现类必须可序列化（Transform 会被序列化分发到集群节点）。
 */
public interface FieldMasker extends Serializable {

    /**
     * 脱敏单个字段值。
     *
     * @param value 原值（调用方保证非 null）
     * @return 脱敏后的值（字符类脱敏返回 String；NORMALIZE 保持原类型时返回原类型值）
     */
    Object mask(Object value);
}
