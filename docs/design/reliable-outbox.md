[English](reliable-outbox.en.md) | **简体中文**

# reliable-outbox：基于本地消息表的可靠消息组件

本文描述 reliable-outbox 的设计取舍与对外约定，重点是设计动机和必须满足的契约。实现细节参见
`reliable-outbox-core`，本文不再重复代码。

## 1. 概述

### 1.1 要解决的问题

写业务数据与发送消息原本是两个独立操作。若在两者之间发生宕机，或消息中间件短暂不可用，结果要么是
消息丢失，要么是消息已经发出、对应的数据却未提交。reliable-outbox 采用 Transactional Outbox
（本地消息表）模式解决这一问题：业务数据与消息记录写入同一个本地事务，事务提交后由后台组件完成投递，
失败则进入重试。

该方案不依赖分布式事务，也不依赖 MQ 的事务消息能力。一套关系型数据库，加上任意一家消息中间件，
即可获得端到端的最终一致性。

### 1.2 设计目标

- 业务数据与消息记录本地原子写入。
- 消息中间件可替换，不绑定具体实现。
- 扫描与清理不内置调度，交由接入方的调度框架驱动。
- 已发送的消息可自动清理，死信保留以供人工处理。
- 提供 `sendInTransaction` 与 `sendStandalone` 两个入口，事务语义明确，内部共用同一套流程。
- 多实例并发安全，进程崩溃后可自愈。
- 死信具备监控、告警与人工回放能力。

### 1.3 能力边界

- 投递语义为至少一次。重复必然出现，消费端必须幂等。
- 不保证投递顺序，同一 `partition_key` 内亦然。
- 消息体默认上限 64KB（65,535 字节），headers 上限 1KB（1,024 字节）。
- 时间统一使用 JVM 本地时间，列为 `DATETIME(3)`，不做时区换算。

完整约束见第 10 节。

## 2. 总体方案

业务数据与消息记录写入同一事务以保证原子性；提交后由独立组件投递到 MQ，失败则重试。组件以库的形式
接入业务服务，随业务进程部署。

整体分为四层：

- 接入层：`MessageClient`。业务方自行选择事务边界，调用 `sendInTransaction` 或 `sendStandalone`。
- 核心层：事务感知、Outbox 记录管理、投递调度、重试、清理、死信治理、监控上报。
- 存储层：Outbox 表，与业务库位于同一数据库实例。
- 外部依赖层：消息中间件、调度组件、监控平台。该层全部通过 SPI 隔离。

各模块职责如下：

| 模块 | 职责 |
| --- | --- |
| `MessageClient` | 对外入口 `sendInTransaction` / `sendStandalone`，共用校验、落库与提交后触发 |
| `TransactionExecutor` | 按指定传播行为执行写入，并注册提交回调 |
| `OutboxRepository` | 消息表读写、状态流转、行级租约抢占，也是数据库方言适配的落点 |
| `MessageDispatcher` | 抢占、投递、成功/失败落库、安排重试 |
| `MessagePublisher` | 投递到具体 MQ（SPI） |
| `MessageScanner` | `scan()` 扫描到期 PENDING，是首次投递与重试的唯一调度来源；不内置调度 |
| `OutboxCleaner` | `clean()` / `clean(shardItem, shardTotal)` 分批清理过期 SENT；不内置调度 |
| `DeadLetterHandler` | 死信日志、告警与监控事件 |
| `DeadLetterReplayer` | 死信回放，带状态条件 |
| `MessageListener` | 投递、失败、死信、租约丢失事件，用于指标与告警 |
| `ReliableOutboxProperties` | 统一配置模型 |

## 3. 数据模型

### 3.1 Outbox 表

默认表名 `t_outbox`。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | BIGINT | 自增主键 |
| `msg_id` | VARCHAR(64) | 全局唯一消息 ID，业务幂等键 |
| `biz_code` | VARCHAR(64) | 业务编码，区分来源系统/模块 |
| `topic` | VARCHAR(255) | 目标主题或队列 |
| `partition_key` | VARCHAR(255) | 可选，透传 Broker 用于分区与消费分组，不承诺顺序 |
| `payload` | TEXT | 消息体，上限 65,535 字节 |
| `headers` | VARCHAR(1024) | 附加属性（traceId、tenantId 等），上限 1,024 字节 |
| `status` | TINYINT | 0 PENDING / 1 SENT / 2 DEAD |
| `retry_count` | INT | 已重试次数，不含首次投递 |
| `next_retry_time` | DATETIME(3) | 下次可被扫描的时间，可空 |
| `owner_token` | VARCHAR(64) | 当前持有租约的实例令牌 |
| `owner_expire_at` | DATETIME(3) | 租约过期时间 |
| `created_at` | DATETIME(3) | 创建时间 |
| `updated_at` | DATETIME(3) | 更新时间 |
| `sent_at` | DATETIME(3) | 发送成功时间，清理据此筛选 |
| `last_error` | VARCHAR(1000) | 最近一次失败原因 |

建表语句（MySQL 8）。自定义表名时替换 `t_outbox` 即可：

```sql
CREATE TABLE `t_outbox` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键 ID，自增',
  `msg_id` VARCHAR(64) NOT NULL COMMENT '消息唯一 ID，全局唯一',
  `biz_code` VARCHAR(64) NOT NULL COMMENT '业务编码，用于区分不同业务方或发送场景',
  `topic` VARCHAR(255) NOT NULL COMMENT '消息主题',
  `partition_key` VARCHAR(255) DEFAULT NULL COMMENT '分区键，用于 Broker 分区选择与消费分组，不承诺顺序',
  `payload` TEXT NOT NULL COMMENT '消息体，默认上限 65535 字节',
  `headers` VARCHAR(1024) DEFAULT NULL COMMENT '透传元数据（如 traceId、tenantId、schemaVersion），默认上限 1024 字节',
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT '消息状态：0 待发送/重试、1 已发送、2 死信',
  `retry_count` INT NOT NULL DEFAULT 0 COMMENT '已重试次数',
  `next_retry_time` DATETIME(3) DEFAULT NULL COMMENT '下次重试时间（JVM 本地时间），扫描器据此判断消息是否到期',
  `owner_token` VARCHAR(64) DEFAULT NULL COMMENT '当前投递租约的持有者标识，用于防止并发重复投递',
  `owner_expire_at` DATETIME(3) DEFAULT NULL COMMENT '投递租约过期时间（JVM 本地时间），过期后可被其他实例抢占',
  `sent_at` DATETIME(3) DEFAULT NULL COMMENT '发送成功时间（JVM 本地时间），清理任务按此字段筛选',
  `last_error` VARCHAR(1000) DEFAULT NULL COMMENT '最近一次投递失败的异常信息',
  `created_at` DATETIME(3) NOT NULL COMMENT '记录创建时间（JVM 本地时间）',
  `updated_at` DATETIME(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '记录更新时间（JVM 本地时间），未显式赋值时自动刷新',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_id` (`msg_id`),
  KEY `idx_status_next_retry` (`status`, `next_retry_time`),
  KEY `idx_status_sent` (`status`, `sent_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

设计说明：

- 状态只保留 PENDING、SENT、DEAD 三种。SENDING 不是持久化状态；"正在投递"由 `status = PENDING` 与
  一个未过期的 `owner_token` 共同表示，查询数据库即可判断。少一个状态，即少一类状态不一致。
- `owner_token` 与 `owner_expire_at` 共同承担并发控制与故障恢复，不建议移除。
- `next_retry_time` 表示消息最早可被扫描器拾取的时间：PENDING 的首次投递为
  `created_at + dispatch.initial-grace`，重试为退避计算所得；SENT 与 DEAD 为 `NULL`。
- DEAD 消息保留在原表，不迁移、不自动清理。人工排查后可直接回放，或修正数据后再回放。相应地，DEAD
  堆积需要关注，主表会持续增长。
- `msg_id` 默认由组件生成 32 位十六进制 UUID。由业务指定时，需满足 1~64 位 ASCII 字母、数字、下划线或
  短横线，并在表内唯一。列默认使用 utf8mb4 排序规则；若默认排序规则不区分大小写，唯一约束与等值查询
  同样不区分，需要字节级精确匹配时应显式指定 `COLLATE utf8mb4_bin`。多个业务模块共用一张表且 `msgId`
  可能重复时，应将唯一键改为 `UNIQUE (biz_code, msg_id)`，并在查询中带上 `biz_code`。
- 索引保留两个：`idx_status_next_retry` 服务于到期扫描，`idx_status_sent` 服务于清理
  （`status = 1 AND sent_at < ?`）。不设顺序相关索引；组件不提供顺序能力，多余的排序索引只会带来写放大。

### 3.2 并发正确性依赖的条件更新

方案的正确性主要依赖以下几处条件更新，它们比具体实现更值得检查：

```sql
-- 抢占（扫描器使用）：租约空闲或已过期，且消息确已到期时才成功，保证同一条消息只有一个实例获胜
UPDATE t_outbox
   SET owner_token = ?, owner_expire_at = ?, updated_at = ?
 WHERE id = ? AND status = 0
   AND next_retry_time IS NOT NULL AND next_retry_time <= ?
   AND (owner_token IS NULL OR owner_expire_at < ?);

-- 发送成功：必须仍持有租约；影响 0 行说明租约已丢失，存在重复投递可能
UPDATE t_outbox
   SET status = 1, owner_token = NULL, owner_expire_at = NULL,
       sent_at = ?, retry_count = ?, next_retry_time = NULL, last_error = NULL, updated_at = ?
 WHERE id = ? AND owner_token = ? AND status = 0;

-- 发送失败但可重试：回到 PENDING、释放租约、写入下次重试时间
UPDATE t_outbox
   SET status = 0, owner_token = NULL, owner_expire_at = NULL,
       retry_count = ?, next_retry_time = ?, last_error = ?, updated_at = ?
 WHERE id = ? AND owner_token = ? AND status = 0;

-- 到期扫描：只取 id，避免读取大字段；按 (next_retry_time, id) 稳定排序
SELECT id FROM t_outbox
 WHERE status = 0 AND next_retry_time IS NOT NULL AND next_retry_time <= ?
   AND (owner_token IS NULL OR owner_expire_at < ?)
 ORDER BY next_retry_time, id LIMIT ?;
```

转入 DEAD 的失败更新结构相同，仅将 `status` 置为 2、`next_retry_time` 置为 `NULL`。

主动触发，即提交后快路径与死信回放，使用不带 `next_retry_time` 条件的 `tryAcquire`；扫描器使用带到期
条件的 `tryAcquireDue`，避免抢占刚被推迟的消息。

清理先定位最老的 SENT，再按区间删除：

```sql
SELECT MIN(sent_at) FROM t_outbox WHERE status = 1;
SELECT id FROM t_outbox WHERE status = 1 AND sent_at >= ? AND sent_at < ? ORDER BY sent_at, id LIMIT ?;
DELETE FROM t_outbox WHERE id IN (...) AND status = 1;
```

DELETE 始终带 `status = 1`，因此重复执行或并发执行都不会误删。

### 3.3 表名自定义

表名通过 `reliable-outbox.table-name` 配置，默认 `t_outbox`。约束如下：

- 表示当前数据源下的单个物理表名。启动时确定，运行期不得切换，也不支持按消息或租户选表。
- 必须匹配 `[A-Za-z_][A-Za-z0-9_]{0,63}`，建议使用小写 snake_case，并避开数据库保留字。
- 表名不能作为 `?` 绑定，Repository 必须先校验再拼接到 SQL。写入、查询、抢占、更新、扫描、回放、
  清理，所有语句都使用同一配置表名，不得硬编码 `t_outbox`。
- 组件不负责建表或迁移。自定义表名时，按上方 DDL 替换表名后预先建表；列类型与长度可以放宽（如改用
  `MEDIUMTEXT`），但 `payload` / `headers` 的应用层上限仍由 `limits.*-bytes` 控制。
- 同一业务库上的所有实例必须配置相同表名。默认实现只接受不带 schema/catalog 的限定名；多数据源场景
  需通过自定义 `OutboxRepository` 或 `@Qualifier` 指定数据源。

## 4. 状态机

`status` 以整数存储：0 为 PENDING，1 为 SENT，2 为 DEAD。

| 状态 | 值 | 含义 |
| --- | --- | --- |
| PENDING | 0 | 待首次投递或待重试；首次失败后仍为 PENDING，由扫描兜底 |
| SENT | 1 | 已投递到 MQ，等待清理 |
| DEAD | 2 | 重试次数耗尽或遇到不可重试错误，需人工处理 |

流转共四种：

- PENDING → SENT：主动触发或扫描抢占后投递成功。
- PENDING → PENDING：失败但可重试，更新 `retry_count` 与 `next_retry_time`。
- PENDING → DEAD：不可重试，或重试次数达到上限。
- DEAD → PENDING：人工回放。

不存在 SENDING 状态，也不存在从 PENDING 自动漂移到其他状态的情况。失败后消息停留在 PENDING，扫描器
按 `next_retry_time` 到期后再次投递，成功才转为 SENT。是否为一次"重试"，以 `last_error` 是否为空判断。

## 5. 核心流程

### 5.1 两个入口

组件只有一种消息模型和一套投递流程。两个入口的区别仅在于事务前置条件：

- `sendInTransaction`：要求调用线程已存在对应数据源的事务，以 `MANDATORY` 加入。没有事务立即失败，
  避免业务数据与消息被拆成两个事务。
- `sendStandalone`：以 `REQUIRES_NEW` 另开独立短事务。外层存在事务时会先挂起，即便外层随后回滚，
  该消息也不会撤销。调用方需明确接受这一语义。

### 5.2 统一内部流程

两个入口最终进入同一个内部流程：

1. 校验参数、生成消息 ID、准备初始状态。
2. `TransactionExecutor` 按入口选择 `MANDATORY` 或 `REQUIRES_NEW`。
3. 在事务内插入 PENDING 记录，`next_retry_time = now + dispatch.initial-grace`。`msg_id` 已存在时抛出
   `DuplicateMessageException`。
4. 插入成功后才在当前事务注册 `afterCommit` 回调；插入失败则不注册，异常直接向上抛出。
5. 事务提交后，回调将投递任务提交到有界线程池，调用线程不等待 MQ 返回。
6. 事务回滚时记录随之回滚，回调不触发。
7. 投递抛异常、线程池拒绝或进程崩溃时，记录仍为 PENDING，由扫描器到期后兜底。

两个入口不会产生两套消息类型、状态机或投递实现。`afterCommit` 内不直接投递 MQ，也不向已提交的调用方
传播异常。

### 5.3 快路径与兜底

主路径是 `afterCommit` 的异步投递，正常情况下消息在毫秒级完成投递。扫描是兜底，不是主路径。健康状态
下每轮扫描的候选集应接近空；若持续命中，说明快路径已退化（线程池长期拒绝、回调未触发、实例反复崩溃），
应优先排查快路径，而非调整扫描参数。

新消息的 `next_retry_time` 为 `created_at + initial-grace`（默认 10 秒），用于避免主动投递尚未执行时
被扫描器提前拾取。宽限期与扫描周期相互独立。即使宽限期短于扫描周期也无妨：扫描与主动投递争抢同一把
租约，未抢到的一方直接退出，不会重复投递。

扫描筛选 `status = 0 且 next_retry_time <= now 且租约空闲或已过期` 的 id，抢占后提交到线程池。多实例
同时扫描是安全的，逐行抢占保证只有一个实例获胜，代价仅为空扫。组件不内置调度线程，`scan()` 的调用
时机与频率由接入方通过 `@Scheduled`、xxl-job、ElasticJob、Quartz 等决定。

兜底恢复时延取决于 `initial-grace`、`lease-duration` 与接入方扫描周期：

| 情况 | 恢复时延 |
| --- | --- |
| 快路径失败（拒绝、回调未触发、提交后崩溃） | `initial-grace` + 一个扫描周期内 |
| 抢到租约后崩溃（未写入 `markSent`/`markFailed`） | `lease-duration` + 一个扫描周期内 |
| 发布失败后重试 | 退避时间 + 一个扫描周期 |

按默认 10s / 30s、扫描 5s 计，约为 15s 内、35s 内，以及退避时间加 0~5s。三者中只有扫描周期由接入方
决定，应按业务可接受的恢复时间设定。

### 5.4 重试

投递失败后先判断异常：可重试的按指数退避计算下次时间，不可重试的（`NonRetryablePublishException`）
直接进入 DEAD。可重试的保持 PENDING，清空租约，记录失败原因与 `next_retry_time`。扫描器下次按
`next_retry_time <= now` 拾取后再次投递；次数达到上限则进入 DEAD 并触发告警。

需要注意，重试的执行精度等于扫描周期。`next_retry_time` 表示"最早可被拾取"，而非精确执行时刻。默认
退避 5/15/35/75/135 秒、扫描 5 秒时，实际约落在 5~10、15~20 秒等区间；扫描周期放宽到 30 秒，则会被
对齐到扫描刻度，进入 DEAD 的时间相应后移。若需贴近默认重试窗口，扫描周期不应超过 `retry.initial-interval`
（默认 5 秒）。

### 5.5 清理

只清理 SENT，且 `sent_at` 早于保留时长。DEAD 永不自动删除。

清理按批执行，并限制单轮批次数，避免长事务锁表、拖慢主从复制。每批删除后重新查询，直到不足一批或
达到上限。与扫描一致，这里不内置调度，`clean()` 交由接入方周期调用。

分片版 `clean(shardItem, shardTotal)` 的处理方式：先用 `MIN(sent_at)` 定位最老一条，得到区间
`[minSent, now - retention)`，再按 `shardTotal` 连续等分，每个分片只删除自己的一段。同一轮内各分片读到
的 `minSent` 基本一致，合并后正好覆盖整个区间，不重不漏。

单实例即 `shardTotal = 1`，`clean()` 与 `clean(0, 1)` 是同一条路径。

分片若错峰调用，后调用的分片可能读到已前移的 `minSent`，该轮高位会残留少量未删除行。这不影响最终结果：
最低分片每轮都从全局最老行开始删除，保证持续推进，后续轮次即可收敛，不会永久遗漏。

### 5.6 死信回放

故障修复后，运维通过受权限控制的管理入口触发回放：

1. 以条件更新将 DEAD 改回 PENDING：`WHERE msg_id = ? AND status = 2`。
2. 同时将 `retry_count` 归零、清空租约与错误信息、`next_retry_time` 设为当前时间。
3. 立即投递一次；失败则进入正常重试。

组件只提供这一基础能力。调用权限、原因记录与审计由接入方负责。回放同样可能重复投递，消费端仍需幂等。

## 6. 并发与可靠性

### 6.1 租约

并发控制完全依赖一把行级租约。投递前通过条件更新抢占：仅当 `owner_token` 为空或 `owner_expire_at`
已过期时才能成功，抢到的实例负责发送。

持有者崩溃后，租约到期会被其他实例或扫描器回收。普通发布失败不经过此路径：`markFailed` 会一并清空
租约并写入 `next_retry_time`，锁立即释放，随后按退避重试。只有进程崩溃，或线程在 `markSent` /
`markFailed` 落库之前卡死，才需要等待租约自然过期。两种情况恢复节奏差异明显，排查时需区分。

租约时长只决定同一条消息两次发布之间的最小间隔，不约束 Publisher 的发送耗时。若发布超过剩余租约，
另一实例可能在租约到期后接管并重发。这属于至少一次语义允许的范围，由消费端幂等兜底，无需据租约推算
发送超时。

任务从线程池取出、真正发布之前，会重新读取消息并确认租约仍然有效（`owner_expire_at` 晚于当前时间）。
若已过期，则归还租约、放弃本次投递，交由扫描器重新抢占。该检查只用于避免用过期的租约发布，并非正确性
前提：即使租约在发布途中过期，重复投递也在允许范围内。

抢占成功后不修改状态，避免引入 SENDING 状态与额外的不一致。

清理任务不持有租约。分片之间以时间窗口隔离，删除语句带 `status = 1`，即使因时钟误差产生重叠，也不会
重复删除或误删。

### 6.2 至少一次与重复来源

发布成功但 `markSent` 未能落库（进程崩溃、租约丢失、数据库异常）时，消息会在后续重试中再次投递。
这是至少一次语义的正常代价，不追求恰好一次，消费端幂等是硬性要求。

`markSent` 返回 false 时必须记录 WARN，并通过 `MessageListener.onLeaseLost` 上报。此类重复若被静默
忽略，长期将无法观测。

`MessagePublisher` 的契约是：正常返回代表 Broker 已确认（如 Kafka `acks=all`、Rabbit publisher
confirm）。"已发出但未确认"一律视为失败。

### 6.3 时钟

全链路使用 JVM 本地时间，列统一为 `DATETIME(3)`，代码直接使用 `LocalDateTime`，亚毫秒由数据库舍入。
不做 UTC 换算，也不与 epoch millis 混用。

多实例部署需启用 NTP。存在偏差时，到期判断会提前或推后；`tryAcquireDue` 只保证同一条消息不被重复抢占，
并不保证精确的到期时刻。

### 6.4 顺序性（非目标）

组件明确不保证投递顺序，这是有意划定的能力边界。

`afterCommit` 主动触发、扫描重试、租约回收与人工回放本就是多条并发路径，同一 `partition_key` 内的消息
完全可能乱序到达 Broker。多实例、批量扫描、退避重试与崩溃恢复都会放大乱序：一条消息进入重试后，
其后继消息可能先行投递。

`partition_key` 仅原样透传给 Broker 用于分区与消费分组。组件不会因 key 相同而串行投递，也不维护任何
水位线。

若确需顺序，需在三层上各自保证：

1. 生产端：同一业务实体的状态变更串行提交（分布式锁、单写者或乐观版本），并在 payload/headers 中携带
   单调递增的 `version`。
2. 传输层：`MessagePublisher` 将 `partition_key` 映射为 Broker 分区键，使同一 key 落到同一分区。
3. 消费端：按 `partition_key` 串行消费，或使用 `version` 做条件更新、后写覆盖。

若要求"投递顺序严格等于提交顺序"，已超出本地消息表的能力范围，应改用 CDC（binlog/WAL 订阅）或数据库
原生事务队列，并接受队头阻塞与相应运维成本。

### 6.5 嵌套事务

`sendInTransaction` 只保证 Outbox 记录与调用时参与的当前物理事务一起提交，不保证这是最外层。若业务
方法自身开启了 `REQUIRES_NEW`，则加入的是内层事务，内层一提交即触发回调，外层后续回滚也无法撤销消息。

`sendStandalone` 固定使用 `REQUIRES_NEW`，从外层事务调用时会挂起外层并独立提交，因此不适用于期望随
外层回滚的场景。

多数据源时需注意：`TransactionExecutor` 必须使用与 `OutboxRepository` 相同、且能参与业务事务的事务
管理器。线程上存在任意 Spring 事务，并不代表 `sendInTransaction` 的前置条件已满足。

### 6.6 多实例扫描

无需选主。逐行抢占保证同一条消息不会被重复投递。

多实例同时扫描是安全的，只会带来空扫与更新争抢，属性能开销而非正确性问题。扫描查询是确定的
（`ORDER BY next_retry_time, id LIMIT batch`），各实例读到同一批队头，逐行 `tryAcquireDue` 使每条消息
只有一个实例成功，其余实例多执行一次未命中的 `UPDATE`。

建议每个实例都调度扫描。多出的空扫成本很低，换来的是只要还有一个实例存活，兜底就不会中断。仅在确有
需要时才收窄到单实例扫描，并配套选主或健康检查，否则该实例故障会使兜底停摆。

错峰抖动只在多实例同时调度时才有意义，用于缓解同批部署、同周期实例长期相位对齐的问题。做法是随机化
首次延迟（如 `[0, 扫描周期)` 内取值），并叠加 ±10%~20% 的周期抖动。若只有一个实例在扫描，无需抖动，
固定周期更稳定，抖动反而会拉长兜底的尾延迟。

抖动只减少空扫与争抢，不改变正确性。即便多个实例同时扫到同一行，租约的条件更新仍只放行一个。

### 6.7 背压

投递线程池为固定大小、有界队列，拒绝策略使用 `AbortPolicy`。

被拒绝时，消息保持 PENDING，交由扫描兜底。扫描器抢到之后若提交仍被拒绝，应立即释放租约，避免空等一个
完整的租约周期。

队列不得无界，否则 MQ 变慢时内存会持续堆积。

### 6.8 消息大小

默认单条 payload 上限为 65,535 字节（`limits.payload-bytes`，对齐 MySQL `TEXT`），headers 上限
1,024 字节（对齐 `VARCHAR(1024)`）。

序列化由调用方负责。组件接收的 `payload` 字符串即落库、投递的最终形态，`validate()` 校验的也是这份
最终字节，因此不存在"转换前很小、转换后超限"的问题，组件中间不做任何转换。

超限时抛出 `MessageTooLargeException`，异常携带实际字节数与上限，且不会写入 Outbox，也不进入重试。
它是运行时异常，在业务事务中抛出会触发回滚；不得在事务内捕获后继续提交业务数据，否则会出现"业务成功、
消息缺失"的静默不一致。

应用层上限不得超过列上限。若需放大大消息，应将列改为 `MEDIUMTEXT`、放宽启动校验，并重新评估 Broker 的
`max.message.bytes`、MySQL 的 `max_allowed_packet`（建议 1MB 以上，需覆盖 payload + headers 与 SQL
开销）以及消费端的反序列化限制。

边界测试至少覆盖：payload 恰好 65,535 通过、65,536 拒绝；headers 1,024 通过、1,025 拒绝；多字节 UTF-8
按字节数计；超限后 Outbox 不新增记录。

更大的消息应改用 Claim Check：消息体存入对象存储，payload 只保留引用。该能力暂未内置。

## 7. 重试策略

采用指数退避加随机抖动：

```
第 k 次重试的等待间隔 = min(初始间隔 × 2^(k-1), 最大间隔) + 随机抖动（k 从 1 开始）
```

默认初始间隔 5 秒、最大间隔 60 秒、最多重试 5 次（不含首次）、抖动上限 1 秒。

不含抖动的时间轴如下（首次投递记为第 0 次）：0s → #1 5s → #2 15s → #3 35s → #4 75s → #5 135s。第 5
次仍失败时 `retry_count` 达到 5，进入 DEAD，即约 135 秒（两分多钟）后可见死信。

抖动的作用是打散到期时刻。消息容易在同一时刻集中到期：同一事务或同一秒创建的消息共享首次到期时间；
Broker 故障时同批 in-flight 消息同时失败；扫描周期与退避间隔整除时，各代重试又会锁步落在同一时间戳。
抖动只负责去同步，不是限流手段；真正的重试风暴防护依赖分批扫描、有界队列与逐行抢占。

默认 1 秒由两个约束共同决定：一方面不超过初始间隔的 25%（5 秒的四分之一），避免扭曲退避节奏；另一方
面不小于消息集中的秒级粒度，否则同一扫描窗口内的突发无法错开。抖动为 0 时必须跳过随机数生成，否则
`nextLong(0, 0)` 会抛异常。

`maxCount = 5` 只统计重试，不含首次，因此总共最多 6 次投递。库中 `retry_count` 采用同一口径，首次成功
或失败时为 0，避免"配置 5 次、库里看到 6"的歧义。人工回放会将其清零，相当于赋予一份新的预算。

默认值背后的判断是：重试只用于吸收瞬时抖动（网络瞬断、单个 Broker 重启、路由刷新，约 30~90 秒），死信
才是人工介入的入口。两分多钟的窗口与 Kafka `delivery.timeout.ms` 默认的 2 分钟大致同量级。更长的故障
属于持续性问题，基础设施监控早已告警，组件继续重试不会提供新的决策信息，只会推迟死信告警。若接入环境
常见故障窗口更长，应显式调大 `retry.max-count`；`retry.max-interval` 默认 60 秒，可将长尾继续约束在
分钟级。

不可重试错误不参与退避，直接进入 DEAD。

## 8. 接口与扩展点

### 8.1 核心契约

对外保持稳定的是以下接口。JDBC 仓储、Dispatcher、Scanner、Cleaner 等属于内部细节，参见
`reliable-outbox-core`。

```java
public interface MessageClient {
    /** 加入已存在的业务事务（MANDATORY）。msg_id 已存在抛 DuplicateMessageException。 */
    String sendInTransaction(OutboxMessage message);
    /** 使用独立的 REQUIRES_NEW 事务。 */
    String sendStandalone(OutboxMessage message);
}

public interface MessagePublisher {   // SPI，由接入方或官方适配器实现
    /** 正常返回即 Broker 已确认；可重试抛 PublishException，不可重试抛 NonRetryablePublishException。 */
    void publish(OutboxMessage message) throws PublishException;
}

public interface MessageListener {    // SPI，用于指标与告警；可能被投递线程并发回调，实现需线程安全
    default void onSent(OutboxMessage message) {}
    default void onFailed(OutboxMessage message, int retryCount, Throwable error) {}
    default void onDead(OutboxMessage message) {}
    default void onLeaseLost(OutboxMessage message, String reason) {}
}

public interface TransactionExecutor {
    <T> T executeInExistingTransaction(Supplier<T> action);  // MANDATORY
    <T> T executeInNewTransaction(Supplier<T> action);       // REQUIRES_NEW
    void afterCommit(Runnable action);                       // 必须在上述事务执行期间调用
}

public interface OutboxRepository {   // 存储抽象，并发正确性与方言适配都在这里
    void insert(OutboxMessage message);
    OutboxMessage findById(Long id);
    OutboxMessage findByMsgId(String msgId);
    boolean tryAcquire(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime now);
    boolean tryAcquireDue(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime dueAt);
    boolean releaseLease(Long id, String ownerToken, LocalDateTime now);
    boolean markSent(Long id, String ownerToken, LocalDateTime sentAt, int retryCount);
    boolean markFailed(Long id, String ownerToken, String error, LocalDateTime nextRetryTime,
                       int retryCount, boolean dead, LocalDateTime now);
    boolean resetForReplay(String msgId, LocalDateTime nextRetryTime, LocalDateTime now);
    LocalDateTime minSentAt();
    List<Long> scanSentBetween(LocalDateTime from, LocalDateTime to, int limit);
    List<Long> scanDispatchableIds(int limit, LocalDateTime now);
    int deleteSent(List<Long> ids);
}
```

与调度相关的组件同样只提供能力、不内置调度：

```java
public class MessageScanner     { public void scan(); }                          // 接入方周期调用
public class OutboxCleaner      { public void clean(); public void clean(int shardItem, int shardTotal); }
public class DeadLetterReplayer { public boolean replay(String msgId); }         // 建议置于受权限控制的管理入口之后
```

### 8.2 配置

前缀统一为 `reliable-outbox`，各组在启动时分别校验，报错携带完整 kebab key：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `table-name` | `t_outbox` | Outbox 表名，见 3.3 |
| `dispatch.threads` | 8 | 默认投递线程池大小，仅默认 executor 生效 |
| `dispatch.queue-capacity` | 1000 | 默认线程池有界队列容量，仅默认 executor 生效 |
| `dispatch.lease-duration` | 30s | 租约时长，同时决定崩溃恢复时延 |
| `dispatch.initial-grace` | 10s | 新消息最早可被扫描的时间偏移，仅首次投递生效 |
| `scan.batch-size` | 100 | 单次扫描查询最多返回条数 |
| `scan.max-batches-per-run` | 10 | 单轮扫描最多处理批次数 |
| `retry.max-count` | 5 | 最大重试次数（不含首次） |
| `retry.initial-interval` | 5s | 首次重试的退避基数 |
| `retry.max-interval` | 60s | 退避间隔上限 |
| `retry.jitter` | 1s | 重试抖动上限 |
| `clean.retention` | 7d | SENT 保留时长 |
| `clean.batch-size` | 1000 | 单批删除条数 |
| `clean.max-batches-per-run` | 10 | 单轮清理最多处理批次数 |
| `limits.payload-bytes` | 65535 | payload 字节上限，不得超过列上限 |
| `limits.headers-bytes` | 1024 | headers 字节上限，不得超过列上限 |

约定：

- 启动校验覆盖几类易静默失效的配置：线程数、队列容量、批次大小、重试次数均需 ≥ 1；
  `retry.initial-interval ≤ retry.max-interval`；`retry.jitter ≥ 0`；`clean.retention > 0`；
  `limits.payload-bytes ≤ 65535`、`limits.headers-bytes ≤ 1024`。后两项与默认 DDL 对齐，避免应用放行、
  数据库插入时失败。
- `lease-duration` 不限制 Publisher 发送耗时，也不需要保留余量；投递前只校验租约是否仍然有效。
- `initial-grace` 只是快路径的去抖动窗口，不是正确性参数。扫描器即便抢先也不会重复投递（有租约保证）。
  取值可按快路径排队时间估算：`queueCapacity / threads × 发布耗时 P`，默认约为 125 倍 P。
- `scan` / `clean` 仅提供单轮批次参数；周期由接入方决定，组件不提供。
- `threads` / `queue-capacity` 仅对默认 `reliableOutboxDispatchExecutor` 生效；接入方覆盖该 bean 后不再
  生效。

### 8.3 扩展点

| 扩展点 | 职责 | 常见实现 |
| --- | --- | --- |
| `OutboxRepository` | 表读写与租约抢占，适配方言与表结构 | MySQL / PostgreSQL / Oracle |
| `TransactionExecutor` | 执行 MANDATORY / REQUIRES_NEW 事务并注册提交回调 | Spring Transaction / 其他事务框架 |
| `MessagePublisher` | 投递到具体 MQ，正常返回即已确认 | Kafka / RocketMQ / RabbitMQ / Pulsar |
| `MessageListener` | 成功、失败、死信、租约丢失事件，用于指标与告警 | Micrometer / 钉钉 / 企微 / 邮件 |

官方 MQ 适配器：

- `reliable-outbox-kafka`：复用 Spring Boot 的 `KafkaTemplate`（存在即装配），确认超时由
  `reliable-outbox.kafka.send-timeout` 控制，默认 5 秒。
- `reliable-outbox-rocketmq`：优先复用 `DefaultMQProducer` bean，没有则按
  `reliable-outbox.rocketmq.namesrv-addr` / `producer-group` 创建并启动，`send-timeout` 默认 5 秒。
  该适配器不处理 `partitionKey`（组件不保证顺序）。

需要注意两点。同一应用只应启用一个适配器；多适配器共存时通过各自的 `reliable-outbox.<broker>.enabled=false`
关闭多余项，或由业务方提供路由用的 `MessagePublisher`。若未引入任何适配模块，则必须自行提供
`MessagePublisher`，否则应用启动失败并明确提示缺失项。

`MessagePublisher` 应自行设置发送超时，避免线程或连接长期阻塞。发布耗时超过剩余租约最多增加一次重复，
不影响至少一次语义。

信封（topic、partition key、headers、msgId）由组件统一提供，各适配器负责映射到具体 MQ 属性。序列化在
调用方，组件原样投递并按字节上限校验。若 Broker 没有稳定的分区语义（如 RocketMQ 队列由 Broker 发送时
负载均衡分配），适配器可忽略 `partition_key`。

多数据源场景需显式指定业务库的 `DataSource`（`@Qualifier` 或自定义 bean 覆盖）。

### 8.4 自动装配

组件通过 Spring Boot 自动配置装配，所有 bean 均带 `@ConditionalOnMissingBean`，`MessagePublisher`、
`OutboxRepository`、`MessageListener` 等均可按需覆盖。

仅存在单个 `PlatformTransactionManager` 时，自动创建 `SpringTransactionExecutor`。存在多个时，必须自行
提供与 Outbox 数据源匹配的 `TransactionExecutor`，否则 `sendInTransaction` 可能绑定到错误的事务。

自动配置不创建调度线程池，也不内置扫描/清理调度，因此无需 `@EnableScheduling`。唯一带有线程的是
`reliableOutboxDispatchExecutor`，用于异步投递。重试同样只由扫描器驱动。

## 9. 部署与运维

### 9.1 部署

以库依赖接入业务服务，随进程部署。Outbox 表与业务库位于同一数据库实例，这是本地事务原子提交的前提。

扫描与清理在每个实例上各自调度即可，逐行抢占保证不会重复投递，无需选主。

所有实例需启用 NTP。列统一为 `DATETIME(3)`，代码以 `LocalDateTime` 绑定，不做时区换算。

`MessagePublisher` 的发送超时宜明显小于 `dispatch.lease-duration`，例如 5~10 秒。如此，发送卡住时可在
租约过期前抛异常、走失败落库并立即释放租约，将"卡死恢复"从等待租约过期缩短到秒级。扫描周期也建议不
超过租约时长，否则崩溃恢复主要受扫描周期支配。

多个实例都调度 `scan()` 时，应加入随机首次延迟与周期抖动（见 6.6）；仅单实例扫描则无需抖动。扫描周期
同时决定兜底恢复速度（见 5.3），应按业务可接受的恢复时间设定。

### 9.2 监控指标

建议通过 `MessageListener` 接入 Micrometer，重点关注：

- Outbox 总行数与各状态数量。
- PENDING 积压量、最长滞留时间，以及按 `retry_count` 的重试分布。
- DEAD 数量，应作为关键告警指标。
- 投递成功率、平均投递延迟、发布超时次数。
- 扫描耗时、单轮扫描条数、线程池拒绝次数，以及 `markSent` 失败次数（重复投递的风险信号）。
- 清理任务的执行频率与删除行数。

### 9.3 告警

- DEAD 一旦出现立即告警，内容携带 `msgId / bizCode / topic / retryCount / lastError`。
- PENDING 积压超过阈值、投递成功率低于阈值。
- `markSent` 失败、线程池持续拒绝、扫描长时间未执行。
- Outbox 表持续增长；DEAD 数量增长单独告警。

告警需去重与限流，避免死信风暴。

### 9.4 清理

SENT 默认保留 7 天，随后分批删除。DEAD 不删除、不迁移；人工排查后回放，或修正数据后再回放，消息始终
保留在原表。

## 10. 约束与演进

### 10.1 已确定的约束

- 投递语义为至少一次，消费端幂等是硬性要求。重试、租约回收与崩溃恢复都会产生重复。
- 不保证顺序，同一 `partition_key` 内亦然。需要顺序时，生产端串行提交、Broker 按 key 分区、消费端按
  key 串行或按版本号条件更新，均需自行实现。
- 大消息：默认 payload ≤ 65,535 字节、headers ≤ 1,024 字节，超出抛 `MessageTooLargeException` 且不落库。
  Claim Check 暂未内置。
- 事务：`sendInTransaction` 以 MANDATORY 加入当前物理事务，`sendStandalone` 以 REQUIRES_NEW 独立提交。
  两者都只对自身实际参与的事务边界负责。
- 时间：数据库统一 `DATETIME(3)`（本地时间，毫秒精度），代码直接使用 `LocalDateTime`，不做时区换算。
- 多实例：由租约抢占保证正确性，无需选主；扫描与清理的调度交由接入方。
- 发布契约：`publish` 正常返回即 Broker 已确认。发送超时由 Publisher 自行设置，outbox 不约束具体数值；
  发布超出租约只会增加重复，不影响至少一次语义。

### 10.2 演进方向

- 补充更多 MQ 适配，完善监控面板与运维工具。
- 评估 Claim Check 大消息与批量回放。
- 若确需严格投递顺序，优先评估 CDC（binlog/WAL 订阅）或数据库原生事务队列，不将本地消息表用作顺序通道。
- 是否提供管理端接口（死信列表、详情、回放、指标查询）及配套权限模型，尚待确定。
