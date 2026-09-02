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

package org.apache.seatunnel.connectors.seatunnel.file.config;

/** 校验文件 MD5 比对不一致时的处理动作。注意：仅对 MD5 生效，声明条数只上报、不参与校验。 */
public enum VerifyMismatchAction {
    /** 仅打印 ERROR 日志，数据照常读取。 */
    WARN,
    /** 抛出异常终止作业，坏包不进入数据管道。 */
    FAIL;
}
