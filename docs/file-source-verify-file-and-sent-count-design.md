# 文件同步校验文件解析与「发送数据量」指标设计方案

> 适用连接器：SftpFile / HdfsFile / LocalFile / S3File / OssFile / CosFile / ObsFile / FtpFile / JindoOssFile —— 实现落在 `connector-file-base`，这些连接器共用 `MultipleTableFileSourceSplitEnumerator`，自动获得该能力
> **不适用**：Hive source —— `MultipleTableHiveSourceSplitEnumerator` 是独立实现，不继承 file-base 的 Enumerator
> 适用引擎：**仅保证 Zeta**
> 状态：已实现，编译与测试通过

---

## 一、需求背景

上游投放数据时，同步目录下成对出现两个文件：

| 文件 | 说明 |
|---|---|
| `xxx.tar.gz` | 数据文件（压缩包，内含一个或多个数据文件） |
| `xxx.success` | 校验文件（后缀可配），单行内容：`压缩包名称 \| 压缩包内数据总条数 \| 压缩包MD5 \| ...` |

需求点：

1. 解析校验文件，取出「压缩包内数据总条数」，定义为**发送数据量**；
2. 该指标需要在**日志和接口**中体现，与现有的**读数据量**、**写数据量**处于同一级别；
3. 校验文件末尾后续可能追加新字段，解析方案必须向后兼容。

---

## 二、现状代码事实

以下均为逐字源码确认，非推测。

| 环节 | 位置 | 现状 |
|---|---|---|
| 指标名常量表 | `seatunnel-api/.../metrics/MetricNames.java:24-42` | 仅有 `SourceReceivedCount` / `SinkWriteCount` / `SinkCommittedCount` 三组，无「发送量」概念 |
| 指标累加 | `engine-server/.../metrics/ConnectorMetricsCalcContext.java:102-122,177` | 按 `PluginType` 初始化指标组，逐行 `inc()` |
| 逐行计数入口 | `engine-server/.../task/SeaTunnelSourceCollector.java:93-116` | 每 `collect` 一行调用一次 `updateMetrics(row, tableId)` |
| 文件列表扫描 | `connector-file-base/.../reader/AbstractReadStrategy.java:167-223` | 递归下钻子目录；**仅过滤 `_SUCCESS`、`.` 开头文件、`filename_extension` 不匹配项** |
| 压缩包读取 | `connector-file-base/.../reader/AbstractReadStrategy.java:354-455` | `ArchiveCompressFormat.TAR_GZ` 已支持（396-414 行），逐 entry 流式读取 |
| 分片策略 | `connector-file-base/.../split/FileSplitStrategyFactory.java:40-44` | **只要 `archive_compress_codec != NONE` 即强制 `DefaultFileSplitStrategy`** |
| 分片对象 | `connector-file-base/.../split/FileSourceSplit.java:28-93` | `readObject():53-60` 已有旧 checkpoint 兼容处理的先例 |
| 客户端日志 | `engine-client/.../job/JobMetricsRunner.java:69-93,105-109` | `JobMetricsSummary` 仅 3 个 long 字段，日志表格硬编码 |
| 指标上报入口 | `SourceReader.Context` / `SourceSplitEnumerator.Context` | 均有 `getMetricsContext()`；Zeta 为真实实现 |
| 其他引擎 | `translation-base/.../CoordinatedReaderContext.java:72-76` | **返回 `new AbstractMetricsContext() {}` 空实现（社区 TODO #3431）** |

### 两条决定性结论

1. **`.success` 后缀文件当前会被当成数据文件读取。**
   `collectFileNamesByPath():188` 只硬编码挡了 `_SUCCESS` 这个精确文件名，`xxx.success` 会通过过滤进入数据文件列表，被 CSV/JSON 解析器读取，产生解析异常或脏数据。这是本次必须一并修复的问题。

2. **tar.gz 永远不会被切分，一个压缩包 = 一个 split = 一个 reader 独占。**
   由 `FileSplitStrategyFactory:40-44` 保证。因此条数上报与 MD5 校验都能在单个 reader 内闭合，不存在跨 subtask 拼接或重复累加的问题，大幅简化实现。

---

## 三、设计决策

| # | 决策项 | 结论 | 理由 |
|---|---|---|---|
| 1 | 指标定位 | 可配置 `verify_on_mismatch = warn \| fail`，默认 `warn` | 对账类需求最终必然要求能 fail，一个 enum 的成本，一次做到位 |
| 2 | 数据文件与校验文件配对 | 解析校验文件内容中的**「压缩包名称」字段**做权威配对 | 不依赖文件名约定，上游用 `checksum_20260902.success` 这类命名也能工作；天然支持一个目录内 N 对 |
| 3 | 校验文件格式 | 单行分隔符，字段**按下标**取值 | 契合「后面可能还有其他内容」——末尾追加字段不影响既有下标解析 |
| 4 | MD5 校验 | 独立开关 `verify_md5_enabled`，**默认关闭**；开启时**前置**完整校验 | 算 tar.gz 的 MD5 必须完整读一遍原始字节（HDFS `getFileChecksum` 是 MD5-of-MD5-of-CRC，不等于文件 MD5；SFTP 无此能力），无捷径。前置校验才有意义——流式旁路要读完才知道错，届时数据已进下游 |
| 5 | 条数的用途 | **仅作为指标上报，完全不参与校验逻辑**。`verify_on_mismatch` **仅对 MD5 生效** | 已确认：条数不做任何一致性判定，不与 `SourceReceivedCount` 或 `SinkWriteCount` 比对，不影响作业成败。MD5 不匹配是文件客观损坏，才需要闸门 |
| 6 | 指标暴露层级 | 完整接入 metrics 体系（跨 4 模块） | 满足「与读/写同级别」的原始诉求 |
| 7 | 引擎范围 | **仅保证 Zeta** | Flink/Spark 的 `MetricsContext` 是社区未完成的空实现，填 #3431 是独立工程，不混入本需求 |
| 8 | 实现位置 | Enumerator 解析一次 + 结果随 split 下发，Reader 侧上报 | 解析 IO 只做一次；指标与 `SourceReceivedCount` 落在同一 vertex，聚合展示时天然并排 |

### 附带收益：投放完成闸门

第 2 条决策衍生出一个重要行为——**没有配对校验文件的数据文件一律跳过不读，等下一轮作业**。

这让本功能同时成为「投放完成闸门」：SFTP/HDFS 落地文件不是原子操作，上游正在传输 `data.tar.gz` 的过程中，`collectFileNamesByPath` 完全可能扫到它（只要 `getLen() > 0`），进而读到一个截断的压缩包。以校验文件的存在作为投放完成信号，从根本上规避该问题。此收益的实际价值可能高于指标本身。

---

## 四、实现方案

### 4.1 `seatunnel-api`

`common/metrics/MetricNames.java` 第 28 行后新增：

```java
public static final String SOURCE_SENT_COUNT = "SourceSentCount";
```

### 4.2 `connector-file-base`（功能主体）

**新增配置项** —— `config/FileBaseSourceOptions.java`，共 7 项，全部默认关闭，不影响任何现有作业。

**新增解析组件**

- `VerifyFileMeta`：承载 `packageName` / `declaredCount` / `expectedMd5` / `rawFields`（原始字段数组，便于后续扩展取用）
- `VerifyFileParser`：按 `verify_file_delimiter` 切分单行，按 `verify_field_index_*` 下标取值；下标配 `-1` 表示该字段不取；字段数不足时按缺失处理并告警，不抛异常

**修改 `reader/AbstractReadStrategy.java`**

- `collectFileNamesByPath():188` —— 在现有 `_SUCCESS` 过滤旁，新增剔除命中 `verify_file_suffix` 的文件，防止校验文件被当作数据文件解析

**修改 `split/MultipleTableFileSourceSplitEnumerator.java`**

- 扫描目录，识别全部校验文件并逐个解析，建立 `压缩包名称 → VerifyFileMeta` 映射
- **仅为映射中已声明的数据文件生成 split**；未被任何校验文件声明的数据文件打 INFO 日志跳过
- 校验文件声明了但对应数据文件不存在 → 打 WARN 跳过，不失败（属清理时序问题，不应中断作业）

**修改 `split/FileSourceSplit.java`**

- 新增 `declaredCount`（long）与 `expectedMd5`（String）字段
- 按 `readObject():53-60` 的既有先例扩展旧 checkpoint 兼容逻辑

**修改 `source/reader/MultipleTableFileSourceReader.java`**

- 若 `verify_md5_enabled = true`：处理 split 前完整读一遍压缩包计算 MD5，与 `expectedMd5` 比对；不匹配则按 `verify_on_mismatch` 处理（`warn` 打 ERROR 日志后照常读，`fail` 抛 `FileConnectorException` 终止）
- MD5 环节结束后（`warn` 模式下即便不匹配也执行）：`context.getMetricsContext().counter(SOURCE_SENT_COUNT).inc(declaredCount)`。**该上报不依赖任何校验结果，条数只上报、不判定**
- 只上报全局 `SourceSentCount`，本期不做 `SourceSentCount#tableName` 的 per-table 拆分，原因见第八章限制第 5 条

### 4.3 `seatunnel-engine-server`

- `metrics/ConnectorMetricsCalcContext.java`：`SourceSentCount` **不走逐行累加路径**，无需改动累加逻辑，仅需确认聚合不丢该 key
- `rest/service/BaseService.java:271` `getJobMetrics()`：聚合新指标

### 4.4 `seatunnel-engine-client`

- `job/JobMetricsRunner.java` `JobMetricsSummary`：新增 `sourceSentCount` 字段，排在首位（发送 → 读 → 写 → 提交）
- `job/JobMetricsRunner.java` 滚动进度日志表格：新增 `Sent Count So Far`，排在 `Read Count So Far` 之前
- `job/JobClient.java` `getJobMetricsSummary()`：解析 `SourceSentCount` 并累加各 subtask 的上报值

### 4.5 `seatunnel-core/seatunnel-starter`

- `command/ClientExecuteCommand.java`：作业结束的 `Job Statistic Information` 汇总表新增 `Total Sent Count`，排在 `Total Read Count` 之前。这是作业跑完后最终落到日志里的那张表，实际比滚动进度日志更常被用来对账。

改动后运行中的滚动进度日志形如：

```
Job Progress Information
------------------------------------------
Job Id                        : 881432421...
Sent Count So Far             : 100000
Read Count So Far             : 100000
Write Attempt Count So Far    : 100000
Write Committed Count So Far  : 100000
Commit Rate                   : 100.00%
...
```

作业结束时的最终汇总日志形如：

```
Job Statistic Information
------------------------------------------
Start Time         : 2026-09-02 10:00:00
End Time           : 2026-09-02 10:02:13
Total Time(s)      : 133
Total Sent Count   : 100000
Total Read Count   : 100000
Total Write Count  : 100000
Total Failed Count : 0
```

---

## 五、关键流程

```
Enumerator（一次）
  ├─ 扫描 path，按 verify_file_suffix 识别出全部校验文件
  ├─ 逐个解析 → Map<压缩包名称, VerifyFileMeta{条数, MD5}>
  ├─ 扫描数据文件列表（已剔除校验文件本身）
  ├─ 未被声明的数据文件 → INFO 跳过，等下一轮
  ├─ 声明了但不存在的数据文件 → WARN 跳过
  └─ 为已声明的数据文件生成 split，携带 declaredCount / expectedMd5
        │
        ▼
Reader（每个 split）
  ├─ verify_md5_enabled ? 完整读一遍算 MD5 → 比对
  │     └─ 不匹配 → warn: 打 ERROR 继续 / fail: 抛异常终止
  ├─ counter(SourceSentCount).inc(declaredCount)
  └─ resolveArchiveCompressedInputStream 正常解压读取
        └─ 每行 collect → SourceReceivedCount 逐行 inc（现有逻辑）
```

---

## 六、配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `verify_file_enabled` | boolean | `false` | 总开关。关闭时行为与现状完全一致 |
| `verify_file_suffix` | string | `.success` | 校验文件识别后缀 |
| `verify_file_delimiter` | string | `\|` | 单行字段分隔符 |
| `verify_field_index_name` | int | `0` | 「压缩包名称」字段下标（从 0 开始） |
| `verify_field_index_count` | int | `1` | 「数据总条数」字段下标。**该值仅用于 `SourceSentCount` 指标上报，不参与任何校验** |
| `verify_field_index_md5` | int | `2` | 「MD5」字段下标，`-1` 表示不取 |
| `verify_md5_enabled` | boolean | `false` | MD5 校验开关。开启 = 双倍 IO |
| `verify_on_mismatch` | string | `warn` | `warn` / `fail`，**仅对 MD5 生效** |

---

## 七、边界情况处理

| 场景 | 处理方式 |
|---|---|
| 数据文件无配对校验文件 | INFO 日志跳过，不生成 split，等下一轮作业 |
| 目录下有多个校验文件 | **每一个都会被读取解析**，声明结果合并到同一个「包名 → 元信息」映射；含子目录中的校验文件（递归收集） |
| 多个校验文件重复声明同一包名 | 后解析到的覆盖先前的，打 WARN 不失败。**保留哪一条取决于文件系统的列举顺序，不保证确定性**，属上游数据质量问题 |
| 校验文件声明了但数据文件不存在 | WARN 日志跳过，不失败 |
| 校验文件字段数不足（下标越界） | 按字段缺失处理，打 WARN，不抛异常 |
| 校验文件末尾追加新字段 | 不影响，按下标取值天然兼容 |
| tar.gz 内含多个数据文件 | 声明条数为整包总和，一个 split 上报一次总和 |
| 作业 failover 重启 | 不做额外去重。Zeta restore 为 pipeline 级重启，`MetricsContext` 随 TaskGroup 重建归零，`SourceSentCount` 与 `SourceReceivedCount` 行为天然一致 |
| MD5 不匹配且 `on_mismatch=warn` | 打 ERROR 日志，数据照常读取 |
| MD5 不匹配且 `on_mismatch=fail` | 抛 `FileConnectorException`，坏包不进入管道 |

---

## 八、已知限制与风险

1. **引擎限制**：Flink / Spark 上 `getMetricsContext()` 返回空实现（`CoordinatedReaderContext:72-76`，社区 TODO #3431），`SourceSentCount` 会静默丢失。connector 侧代码照常运行，不报错、不影响数据同步，但指标不可见。

2. **指标语义说明（非风险）**：`skip_header_row_number` / `csv_use_header_line` 会使 reader 少吐 N 行（N = 包内数据文件数），因此 `SourceSentCount` 与 `SourceReceivedCount` 可能存在固定偏差。**这是预期行为**——已确认上游声明的条数仅作为指标上报，不参与任何校验逻辑，两者不做比对，偏差不影响作业成败。两个指标的语义分别是「上游声明发送了多少条」与「本作业实际读取了多少条」，本就允许不等。

3. **MD5 性能**：开启 `verify_md5_enabled` 意味着每个压缩包被完整读取两遍。大文件场景需评估 IO 开销后再决定是否开启。

4. **行为变更**：`verify_file_enabled = true` 后，目录下无配对校验文件的数据文件将不再被读取。因总开关默认 `false`，仅影响主动开启该功能的作业。

5. **不做 per-table 拆分**：只上报全局 `SourceSentCount`，不上报 `SourceSentCount#tableName`。`BaseService` 的 table 级聚合是位置硬编码的数组结构（`tableMetricsMaps` 与 `tableCountMetricsNames` 下标一一对应），插入新指标需同步改 `RestConstant`、两个数组及其注释，改动面与回归风险高于收益；且 file source 的多表场景在校验文件模式下极少出现。需求中的「与读/写同级别」指全局指标，已满足。

---

## 九、配置样例

### 9.1 SFTP 源（典型场景）

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  SftpFile {
    host = "10.0.0.10"
    port = 22
    user = "datax"
    password = "******"
    path = "/data/incoming/order"
    file_format_type = "csv"
    field_delimiter = ","
    archive_compress_codec = "tar_gz"
    skip_header_row_number = 1

    # ===== 校验文件与发送数据量 =====
    # 总开关，默认 false，不影响任何现有作业
    verify_file_enabled = true
    # 校验文件识别后缀
    verify_file_suffix = ".success"
    # 单行分隔符 + 字段下标（从 0 开始，-1 表示不取）
    verify_file_delimiter = "|"
    verify_field_index_name  = 0
    verify_field_index_count = 1
    verify_field_index_md5   = 2
    # MD5 校验，默认关（开启 = 双倍 IO）
    verify_md5_enabled = false
    # 仅对 MD5 生效：warn | fail
    verify_on_mismatch = "warn"

    schema {
      fields {
        order_id   = string
        user_id    = bigint
        amount     = "decimal(18,2)"
        created_at = timestamp
      }
    }
  }
}

sink {
  Console {}
}
```

### 9.2 HDFS 源 + 开启 MD5 强校验

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  HdfsFile {
    fs.defaultFS = "hdfs://nameservice1"
    path = "/warehouse/incoming/order"
    file_format_type = "csv"
    archive_compress_codec = "tar_gz"

    # ===== 校验文件与发送数据量 =====
    verify_file_enabled = true
    verify_file_suffix = ".check"
    verify_file_delimiter = ","
    verify_field_index_name  = 0
    verify_field_index_count = 1
    verify_field_index_md5   = 2
    # 开启 MD5 前置校验，坏包不进入管道
    verify_md5_enabled = true
    # MD5 不匹配直接让作业失败
    verify_on_mismatch = "fail"

    schema {
      fields {
        order_id = string
        amount   = "decimal(18,2)"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:mysql://10.0.0.20:3306/dw"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "dw"
    password = "******"
    table = "ods_order"
  }
}
```

### 9.3 对应的目录与校验文件内容

> 以下为**已确认的最终格式**，`verify_*` 各配置项的默认值即按此样例设定。

同步目录 `/data/incoming/order` 下：

```
order_20260902.tar.gz          # 数据文件
order_20260902.success         # 校验文件
order_20260903.tar.gz          # 数据文件（正在传输中，尚无校验文件）
```

`order_20260902.success` 内容：

```
order_20260902.tar.gz|100000|9e107d9d372bb6826bd81d3542a419d6|20260902|batch_a
```

本轮作业行为：

- `order_20260902.tar.gz` 被读取，`SourceSentCount` 上报 `100000`
- `order_20260903.tar.gz` 因无配对校验文件被跳过（INFO 日志），等下一轮作业
- 末尾的 `20260902`、`batch_a` 两个额外字段被忽略，不影响解析

### 9.4 关闭功能（保持现状行为）

不配置任何 `verify_*` 项，或显式关闭：

```hocon
source {
  SftpFile {
    path = "/data/incoming/order"
    file_format_type = "csv"
    archive_compress_codec = "tar_gz"

    verify_file_enabled = false   # 或直接不写
  }
}
```

此时全部行为与当前版本完全一致：校验文件仍按现有规则参与文件列表扫描，不解析、不上报 `SourceSentCount`。

---

## 十、实现落地与测试覆盖

### 10.1 新增文件

| 文件 | 作用 |
|---|---|
| `connector-file-base/.../config/VerifyMismatchAction.java` | `warn` / `fail` 枚举 |
| `connector-file-base/.../source/verify/VerifyFileMeta.java` | 单条声明的元信息（包名、条数、MD5、原始字段） |
| `connector-file-base/.../source/verify/VerifyFileParser.java` | 单行分隔符 + 下标取值的解析器，含 `extractFileName` 配对键归一 |

### 10.2 测试覆盖

| 测试类 | 用例数 | 覆盖内容 |
|---|---|---|
| `VerifyFileParserTest` | 14 | 真实样例解析、末尾追加字段兼容、多行多包、分隔符为正则元字符（`.`/`\|`）、条数非法/负数/空白降级、声明 0 条与缺失的区分、下标 -1、下标越界、包名空白跳过、包名带路径归一、空内容、包名下标为负显式失败、自定义字段顺序、`rawFields` 防御性拷贝 |
| `VerifyFileScanTest` | 8 | 校验文件被剔除且未声明包跳过、总开关关闭时行为零变更、`filename_extension` 不影响校验文件识别、无校验文件时全部跳过、单个校验文件声明多包、**同目录多个校验文件全部生效**、**子目录中的校验文件被递归收集**、**跨文件重复声明同一包名只告警不失败** |
| `JobClientTest`（扩充） | +3 | `SourceSentCount` 正常解析、多 subtask 累加、指标缺失时为 0 |

### 10.3 验证结果

| 范围 | 结果 |
|---|---|
| `seatunnel-api` | 72 个测试通过 |
| `connector-file-base` | 111 个测试通过（含新增 22 个） |
| `seatunnel-engine-client` | `JobClientTest` 13 个通过 |
| `seatunnel-engine-server` | `BaseServiceTableMetricsTest` 10 个、`JobMetricsTest` 3 个、`ConnectorMetricsCalcContextTest` 2 个通过 |
| 编译 | 11 个 file 系 connector + `connector-hive` + `seatunnel-core-starter` 全部 `test-compile` 通过 |

已确认现有 e2e 对 REST metrics 的断言（`RestApiIT`、`MultiTableMetricsIT`、`ClusterSeaTunnelEngineContainer`）都是按字段名取值比对，不校验字段全集，因此新增 `SourceSentCount` 字段不会破坏它们。
