# reliable-outbox —— 基于本地消息表的分布式事务消息组件

## 1. 项目背景与目标

reliable-outbox 是一个基于 Transactional Outbox 模式的可靠消息组件，独立进行开发、构建、测试、版本化和发布，用于解决分布式场景下业务数据与消息发送的一致性问题。

核心目标：

- 业务数据写入与消息记录写入保证本地原子性。
- 消息中间件可自由扩展，不绑定具体 MQ。
- 重试能力可插拔，不绑定具体任务调度平台。
- 已成功发送的消息可自动清理。
- 提供 `sendInTransaction` 与 `sendStandalone` 两个事务语义明确的入口，内部共用同一套消息校验、落库、提交后触发、扫描和重试流程。
- 支持多实例部署下的并发安全与故障恢复。
- 构建完善的死信监控、告警、人工回放机制。

关键约束（详见第 13 节）：

- 投递语义为**至少一次**，消费端必须幂等。
- **不承诺投递顺序**：同一 `partition_key` 内也可能因并发投递、失败重试、租约回收、多实例扫描和崩溃恢复而乱序。需要顺序的业务必须自行保证：生产端串行提交、Broker 按 key 分区、消费端按 key 串行消费或使用版本号做条件更新。
- 默认消息体上限 64KB（`65,535` 字节，对齐 MySQL `TEXT` 容量），超过上限在发送入口直接拒绝；超大消息需改用 Claim Check 等外部方案。
- 所有时间统一使用 UTC，数据库列使用 `DATETIME(3)`（毫秒精度），代码层使用 `Instant` 并显式按 UTC 转换。
- `sendInTransaction` 只保证 Outbox 记录与调用时所参与的当前物理事务一致；`sendStandalone` 使用独立事务，不随调用方外层事务回滚。

## 2. 总体方案

### 2.1 核心思想

reliable-outbox 采用业界成熟的 Transactional Outbox 模式。核心思路是：业务数据和消息记录在同一个数据库事务内写入，保证两者的原子性；事务提交后，由独立的投递组件将消息发送到消息中间件。发送失败则进入可控重试流程，最终保证消息不丢失，实现跨服务的最终一致性。

### 2.2 架构分层

- **接入层**：通过 `MessageClient` 提供 `sendInTransaction` 与 `sendStandalone`，业务方显式选择事务边界。
- **核心层**：事务感知、Outbox 记录管理、消息调度投递、重试控制、清理、死信治理、监控上报。
- **存储层**：Outbox 消息表与业务数据库。
- **外部依赖层**：消息中间件、重试调度组件、监控平台。该层全部通过 SPI 接口隔离，使用者可自行接入。

### 2.3 核心模块

| 模块 | 职责 |
| --- | --- |
| MessageClient | 对外提供 `sendInTransaction`、`sendStandalone` 两个入口，共用消息校验、落库与投递触发逻辑 |
| TransactionExecutor | 按指定传播行为执行 Outbox 写入，并注册事务提交回调 |
| OutboxRepository | 负责消息表的读写、状态流转与行级抢占 |
| MessageDispatcher | 负责消息的抢占、投递、成功/失败状态落库 |
| MessagePublisher | 发送消息到指定 MQ，SPI |
| RetryScheduler | 在指定时间触发单条消息重试的**加速器**，非可靠定时器 |
| MessageScanner | 统一扫描到期的 INIT/FAILED 消息，作为首次投递与重试的**兜底** |
| OutboxCleaner | 分批清理已发送成功的过期消息 |
| DeadLetterHandler | 死信日志、告警与监控事件 |
| DeadLetterReplayer | 死信人工回放，带状态条件 |
| MessageListener | 投递/失败/死信事件监听，用于指标与告警扩展 |
| ReliableOutboxProperties | 统一配置模型 |


## 3. 数据模型设计

### 3.1 Outbox 消息表（默认表名 `t_outbox`）

用于存储待发送、待重试、已发送及死信消息。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 自增主键 |
| msg_id | VARCHAR(64) | 全局唯一消息 ID，业务幂等键 |
| biz_code | VARCHAR(64) | 业务编码，区分来源系统/模块 |
| topic | VARCHAR(255) | 目标 MQ 主题或队列 |
| partition_key | VARCHAR(255) | 分区键，可选；透传给 Broker 作为分区/队列选择依据与消费分组维度，不承诺顺序 |
| payload | TEXT | 消息体内容，上限 65,535 字节（约 64KB），见 6.8 |
| headers | TEXT | 附加属性，可选，上限 4,096 字节，见 6.8 |
| status | VARCHAR(16) | INIT / FAILED / SENT / DEAD |
| deleted | TINYINT(1) | 0 正常，1 已软删除；见设计说明 |
| retry_count | INT | 已重试次数（不含首次投递） |
| next_retry_time | DATETIME(3) | 下次可被扫描的时间（UTC），可空 |
| owner_token | VARCHAR(64) | 当前处理该消息的实例令牌 |
| owner_expire_at | DATETIME(3) | 租约过期时间（UTC） |
| created_at | DATETIME(3) | 创建时间（UTC） |
| updated_at | DATETIME(3) | 更新时间（UTC） |
| sent_at | DATETIME(3) | 发送时间（UTC） |
| last_error | VARCHAR(1000) | 最近一次发送失败原因 |

设计说明：

- 状态精简为 INIT / FAILED / SENT / DEAD 四种，不设置 SENDING；“投递中”的可观测定义是 **`status ∈ {INIT, FAILED}` 且 `owner_token` 非空且租约未过期**。
- `deleted` 是软删除标记（0 正常，1 已删除），与 `status` 正交。扫描、租约抢占、回放和投递前检查都必须附加 `deleted = 0`；已删除消息不会再被投递。软删除不会撤回已经在途的发布，至多仍可能产生一次投递；软删除必须同时更新 `updated_at`。`uk_msg_id` 仍占用该 `msg_id`，复用同一 ID 需先物理清理。
- 软删除对所有状态一致适用：删除任意状态的行都只影响该消息自身，不影响其余消息的投递。
- `owner_token` 与 `owner_expire_at` 用于轻量级并发控制与故障恢复，不建议去除。
- **所有时间字段统一使用 `DATETIME(3)` 存储 UTC 时间**，毫秒精度。代码层保留 `Instant`，读写时显式转换为 UTC 的 `LocalDateTime`（`LocalDateTime.ofInstant(instant, ZoneOffset.UTC)` / `localDateTime.toInstant(ZoneOffset.UTC)`），**不依赖 JVM 默认时区、JDBC 会话时区或驱动配置**。不得与 epoch millis 混用。
- `next_retry_time` 的语义是“最早可被后台扫描器拾取的时间”：
  - INIT：`created_at + reliable-outbox.init-scan-grace`（默认 30 秒），用于主动触发失败后的兜底。
  - FAILED：按重试策略计算出的下次重试时间。
  - SENT / DEAD：`NULL`。
- `DEAD` 状态消息保留在原表，不做归档迁移；但需要监控表增长，生产环境建议增加归档策略（见 10.4）。
- `msg_id` 默认由组件生成 32 位十六进制 UUID；业务自定义时由应用层要求 1~64 位 ASCII 字母、数字、下划线或短横线，且在表内全局唯一。列使用表默认的 utf8mb4 排序规则，若默认排序规则大小写不敏感，唯一约束和等值查询也会大小写不敏感；需要字节级精确匹配时应显式使用 `COLLATE utf8mb4_bin`。若多个业务模块共用一张表且业务 `msgId` 可能重复，应改为 `UNIQUE (biz_code, msg_id)`，并让所有查询携带 `biz_code`。
- `headers` 使用 `TEXT`，上限 **4,096 字节**，作为不透明附加属性原样透传给 MQ。组件不解析、不校验其结构；需要结构化元数据时由调用方自行序列化（如 JSON）。
- `payload` 使用 `TEXT`，上限为 **65,535 字节**。应用层 `max-payload-bytes` 必须 ≤ 65,535，默认取 65,535（即 64KB−1 字节），避免配置成 65,536 时出现“应用放行、数据库拒绝”的边界不一致。若未来需要更大消息，应同时把列类型改为 `MEDIUMTEXT` 并调整该配置，或改用 Claim Check。
- `partition_key` 仅作为 Broker 路由与消费分组维度，组件**不据此提供顺序保证**。需要顺序的业务应在业务层保证同一 key 的提交串行，并在消费端按 key 串行处理或使用版本号做条件更新。

建表语句（MySQL 8，使用默认表名；自定义表名时需将 `t_outbox` 替换为配置值）：

```sql
CREATE TABLE `t_outbox` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键 ID，自增',
  `msg_id` VARCHAR(64) NOT NULL COMMENT '消息唯一 ID，全局唯一',
  `biz_code` VARCHAR(64) NOT NULL COMMENT '业务编码，用于区分不同业务方或发送场景',
  `topic` VARCHAR(255) NOT NULL COMMENT '消息主题',
  `partition_key` VARCHAR(255) DEFAULT NULL COMMENT '分区键，用于 Broker 分区选择与消费分组，不承诺顺序',
  `payload` TEXT NOT NULL COMMENT '消息体，默认上限 65535 字节',
  `headers` TEXT DEFAULT NULL COMMENT '透传元数据（如 traceId、tenantId、schemaVersion），默认上限 4096 字节',
  `status` VARCHAR(16) NOT NULL DEFAULT 'INIT' COMMENT '消息状态：INIT 待发送、FAILED 待重试、SENT 已发送、DEAD 死信',
  `deleted` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '软删除标记：0 正常，1 已删除',
  `retry_count` INT NOT NULL DEFAULT 0 COMMENT '已重试次数',
  `next_retry_time` DATETIME(3) DEFAULT NULL COMMENT '下次重试时间（UTC），扫描器据此判断消息是否到期',
  `owner_token` VARCHAR(64) DEFAULT NULL COMMENT '当前投递租约的持有者标识，用于防止并发重复投递',
  `owner_expire_at` DATETIME(3) DEFAULT NULL COMMENT '投递租约过期时间（UTC），过期后可被其他实例抢占',
  `created_at` DATETIME(3) NOT NULL COMMENT '记录创建时间（UTC）',
  `updated_at` DATETIME(3) NOT NULL COMMENT '记录更新时间（UTC）',
  `sent_at` DATETIME(3) DEFAULT NULL COMMENT '发送成功时间（UTC），清理任务按此字段筛选',
  `last_error` VARCHAR(1000) DEFAULT NULL COMMENT '最近一次投递失败的异常信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_id` (`msg_id`),
  KEY `idx_status_next_retry` (`status`, `next_retry_time`),
  KEY `idx_status_sent` (`status`, `sent_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

索引说明：

- `idx_status_next_retry` 服务于统一的到期扫描（INIT + FAILED）。
- `idx_status_sent` 服务于清理任务（`status='SENT' AND sent_at < ?`），初稿只有 `(status, next_retry_time)`，清理查询无法有效利用。
- 不保留顺序相关索引：组件不提供顺序能力，多余的排序索引只会带来写放大。
- 不再单独保留 `idx_created_at`：积压年龄可通过 `next_retry_time` 估算，减少写放大。

### 3.2 表名自定义

Outbox 表名通过 `reliable-outbox.table-name` 配置，默认值为 `t_outbox`：

```yaml
reliable-outbox:
  table-name: biz_event_outbox
```

约束如下：

- 配置值表示当前数据源下的单个物理表名。表名在应用启动时确定，同一个应用实例运行期间不得动态切换，也不支持按消息或租户动态选表。
- 表名必须匹配 `[A-Za-z_][A-Za-z0-9_]{0,63}`：以字母或下划线开头，只包含字母、数字和下划线，总长度不超过 64 个字符；建议使用小写 snake_case 并避开数据库保留字。
- SQL 标识符不能使用 `?` 占位符绑定。Repository 必须先校验表名，再将其拼接到 SQL；不得把请求参数、消息字段或其他运行时输入作为表名。
- 所有 Repository SQL，包括写入、查询、租约抢占、状态更新、扫描、回放和清理，都必须使用同一个配置表名，不得硬编码 `t_outbox`。
- 组件不负责在运行时自动创建或迁移表。使用自定义名称时，部署方必须以第 3.1 节 DDL 为模板预先建表，只替换表名，不得改变组件依赖的字段、索引和约束。
- 同一业务数据库上的所有应用实例必须配置相同表名。若需要使用不同 schema，应通过数据源选择，或由自定义 `OutboxRepository` 负责方言和限定名处理；默认实现只接受不带 schema/catalog 的表名。

## 4. 消息状态机

### 4.1 状态定义

| 状态 | 含义 |
| --- | --- |
| INIT | 待首次投递；或主动触发失败、等待兜底扫描 |
| FAILED | 发送失败，等待重试 |
| SENT | 已成功投递到 MQ，等待清理 |
| DEAD | 超过最大重试次数或遇到不可重试错误，需人工处理 |

“投递中”不是持久化状态，而是 `INIT/FAILED` + 有效 `owner_token` 的组合。

### 4.2 状态流转

- **INIT → SENT**：主动触发或扫描抢占后投递成功。
- **INIT → FAILED**：投递失败且可重试。
- **INIT → DEAD**：遇到不可重试错误（如消息格式非法、topic 不存在）。
- **FAILED → SENT**：重试成功。
- **FAILED → FAILED**：重试失败，更新 `retry_count` 与 `next_retry_time`。
- **FAILED → DEAD**：重试次数达到上限。
- **DEAD → INIT**：人工修复链路后回放。

说明：

- **不存在 FAILED → INIT 的自动流转**。FAILED 消息由扫描器或重试加速器直接重新投递，状态在成功后变为 SENT、失败后仍为 FAILED。
- 不设置 SENDING 状态，由 `owner_token` 承担并发控制权标识职责。
- DEAD 消息不迁移、不自动清理，只能通过人工回放恢复为 INIT；回放频率与次数由管理端策略控制，不在消息行上记录。

## 5. 核心流程

### 5.1 发送入口与事务语义

组件只定义一种 Outbox 消息模型和一套后续投递流程；两个入口只表达不同的事务前置条件：

- `sendInTransaction`：要求调用线程已存在与 Outbox 数据源对应的事务，并以 `MANDATORY` 传播行为加入该事务。没有对应事务时立即失败，避免业务数据与消息被静默拆成两个事务。
- `sendStandalone`：始终以 `REQUIRES_NEW` 创建独立短事务写入 Outbox。调用线程已有事务时会先挂起外层事务，因此外层随后回滚不会撤销该消息；调用方必须明确接受这一语义。

### 5.2 统一内部发送流程

1. 两个入口都进入同一个内部 `doSend` 流程，完成参数校验、消息 ID 生成和初始状态准备。
2. `TransactionExecutor` 根据入口选择 `MANDATORY` 或 `REQUIRES_NEW` 事务模板。
3. 在选定事务内执行 `insertIfAbsent`，写入 INIT 状态的 Outbox 记录，并设置 `next_retry_time = now + init-scan-grace`。
4. 写入成功后，在当前事务中注册同一个 `afterCommit` 回调；重复 `msg_id` 不重复注册投递任务。
5. 事务提交后，回调把投递任务提交到有界线程池，调用线程不等待 MQ 返回。
6. 事务回滚时，Outbox 记录随当前事务回滚，回调不触发。
7. 投递任务异常、线程池拒绝或实例崩溃时，记录仍为 INIT，由 `MessageScanner` 在 `next_retry_time` 到期后兜底投递。

> 两个入口只决定 Outbox 写入使用哪个事务边界，不产生两套消息类型、状态机或投递实现。`afterCommit` 中不得直接发布 MQ，也不得让异常向已经完成提交的调用方传播。

### 5.3 首次触发与兜底扫描

- 两个发送入口都在 Outbox 写入事务提交后由 `afterCommit` 回调主动触发异步发送，实现毫秒级投递。
- 定时扫描不作为 INIT 的首次触发机制；INIT 的 `next_retry_time` 设置为 `created_at + init-scan-grace`（默认 30 秒，可配），避免主动投递尚未执行时被扫描器重复拾取。结合默认 30 秒的扫描周期，主动触发失效时通常会在消息创建后的 30～60 秒内被兜底扫描发现。
- `MessageScanner` 统一扫描 `status IN ('INIT','FAILED') AND deleted = 0 AND next_retry_time <= now AND 租约空闲/已过期` 的消息 ID，避免扫描时读取大字段 `payload`。
- 扫描器通过逐行 `tryAcquireDue` 抢占后提交到投递线程池，因此**多实例同时扫描是安全的**，只是会产生空扫开销。

### 5.4 重试流程

1. Dispatcher 投递消息失败。
2. 判断异常类型：
   - 可重试异常：按指数退避计算下次重试时间。
   - 不可重试异常（`NonRetryablePublishException`）：直接进入 DEAD，避免无意义重试。
3. 在持有租约的前提下，将消息状态置为 FAILED（或 DEAD），清空 `owner_token`，记录失败原因。
4. 若仍可重试，通过 `RetryScheduler` 注册延迟任务，作为低延迟加速。
5. 到达触发时间，`RetryScheduler` 把任务提交到投递线程池，重新投递。
6. 即使 `RetryScheduler` 失效（进程重启、队列拒绝、时钟提前），`MessageScanner` 也会在 `next_retry_time` 到期后重新拉起该消息。
7. 重试次数超过阈值后，消息置为 DEAD 并触发告警。

> `RetryScheduler` 只是加速器，**不是可靠定时器**：内存中的延迟任务在重启后会丢失，可靠兜底永远是数据库扫描。

### 5.5 清理流程

- 只清理状态为 SENT 且发送时间超过保留时长的消息。
- DEAD 状态消息永不自动清理，保留供人工排查。
- 软删除（`deleted=1`）只影响投递可见性；自动清理仍只处理 SENT，软删除的 DEAD 行继续保留供排查。需要物理清理时由运维/归档任务按 `updated_at` 单独执行。
- 清理任务按批执行，并限制单次运行的最大批次数，避免长事务、锁表和对主从复制造成压力。
- 每批删除后重新查询，直到不足一批或达到单次运行上限。

### 5.6 死信回放流程

1. 运维确认故障已修复，调用回放接口。
2. 通过条件更新执行 `status DEAD → INIT`：`WHERE msg_id = ? AND status = 'DEAD'`。
3. 重置 `retry_count = 0`、清空租约与错误信息、`next_retry_time = now`。
4. 立即触发一次投递；若失败，进入正常重试流程。
5. 回放操作必须记录操作人/原因，并通过受权限控制的管理入口执行；回放本身仍可能重复投递，消费端必须幂等。

## 6. 并发与可靠性

### 6.1 多实例并发控制

- 使用 `owner_token` + `owner_expire_at` 实现轻量级租约。
- 投递前通过条件更新抢占：`owner_token IS NULL OR owner_expire_at < now`，只有抢占成功的实例才执行发送。
- 持有者崩溃后，租约到期由其他实例或扫描器回收。
- **租约时长必须大于 MQ 发布超时**（默认租约 60s、发布超时 20s），否则会出现“发布尚未超时、租约已过期、另一实例重发”的重复投递。
- 任务从线程池出队后、真正发布前，必须检查**剩余租约 ≥ 发布超时**；不足时主动归还租约并放弃本次投递，由扫描器立即重新抢占，避免发布过程中租约被接管。
- 抢占成功后不再修改状态，避免引入 SENDING 状态和额外的状态不一致。

### 6.2 至少一次语义与重复窗口

- 发布成功但 `markSent` 失败（进程崩溃、租约丢失、数据库异常）时，消息会在后续重试中被再次投递。
- 这是至少一次语义的正常代价，**不尝试追求恰好一次**；消费端幂等是硬要求。
- `markSent` 返回 false 时必须记录 WARN 并通过 `MessageListener.onLeaseLost` 上报指标，不能静默忽略，否则重复投递会长期不可见。
- `MessagePublisher` 的契约：正常返回代表 Broker 已确认（如 Kafka `acks=all`、Rabbit publisher confirm）；任何“已发出但未确认”都必须视为失败。

### 6.3 时钟与时区

- 全链路使用 UTC；数据库列统一为 `DATETIME(3)`（毫秒精度）。`Instant` ↔ 数据库时间的转换必须显式指定 UTC，不依赖 JVM 默认时区、JDBC 会话时区或驱动的 `connectionTimeZone`，也禁止与 epoch millis 混用。
- 多实例部署时要求 NTP 同步。
- 重试加速器可能因为时钟提前触发；此时 `tryAcquireDue` 的条件更新不会命中，消息由扫描器在到期后兜底。

### 6.4 顺序性（非目标）

reliable-outbox 明确**不承诺投递顺序**，这是刻意的能力边界，而不是实现缺陷：

- 扫描、`afterCommit` 触发、重试加速、租约回收和人工回放是多条并发路径，同一 `partition_key` 内的消息可能以任意顺序到达 Broker。
- 多实例部署、批量扫描、失败重试退避以及崩溃恢复都会放大乱序概率；一条消息进入 FAILED 后，其后继消息可能先行投递。
- `partition_key` 仅作为 Broker 分区/队列选择与消费分组维度原样透传；组件不会因为相同 key 而串行发布，也不维护任何水位线。

需要业务顺序时的推荐做法：

1. **生产端**：同一业务实体的状态变更必须串行提交（分布式锁、单写者或乐观版本），并在 payload/headers 中携带单调 `version`。
2. **传输层**：`MessagePublisher` 把 `partition_key` 映射为 Broker 分区键，使同一 key 进入同一分区。
3. **消费端**：按 `partition_key` 串行消费，或使用 `version` 做条件更新/后写覆盖；重复与乱序由消费端收敛。

如果业务要求“投递顺序严格等于提交顺序”，这超出了本地消息表的能力范围，应改用 CDC（如 binlog 订阅）或数据库原生事务队列，并接受队头阻塞与运维成本。

### 6.5 嵌套事务

- `sendInTransaction` 只保证 Outbox 记录与调用时所参与的**当前物理事务**原子提交，并不保证它一定是最外层事务。
- 如果业务方法本身使用 `REQUIRES_NEW`，`sendInTransaction` 加入的是该内层事务；内层提交后即触发 `afterCommit`，外层事务随后回滚不会撤销消息。
- `sendStandalone` 固定使用 `REQUIRES_NEW`。即使从外层事务中调用，也会挂起外层事务并独立提交消息；该入口不得用于期望消息随外层业务回滚的场景。
- 多数据源场景必须让 `TransactionExecutor` 使用与 `OutboxRepository` 相同且能参与业务事务的事务管理器，不能仅凭线程上存在任意 Spring 事务就判定满足 `sendInTransaction`。

### 6.6 多实例扫描

- 扫描器无需强制选主：逐行抢占保证同一条消息不会被重复发布。
- 多实例同时扫描会产生空扫和更新竞争；表数据量大、实例数量多时，可选通过 `LockManager`（ShedLock / 数据库锁）减少空扫，但不作为正确性前提。

### 6.7 背压

- 投递线程池固定大小、有界队列，拒绝策略使用 `AbortPolicy`。
- 线程池拒绝时：消息保持在 INIT/FAILED，由扫描器后续兜底；扫描器抢占后若提交被拒绝，应立即释放租约，避免白等一个租约周期。
- 禁止使用无界队列，避免 MQ 故障时内存堆积。

### 6.8 消息体大小

- 默认单条消息体上限 **64KB**，对应 `reliable-outbox.max-payload-bytes=65535`（64KB−1 字节，正好对齐 MySQL `TEXT` 的 65,535 字节上限）。
- 校验发生在**序列化之后、落库之前**：如果接入 `MessageConverter`，必须对转换后的最终 payload 校验，避免“序列化前很小、序列化后超限”。
- 超限时抛出专用异常 `MessageTooLargeException`，异常信息包含实际字节数与上限；此时**不会写入 Outbox**。
- 该异常属于调用方错误，默认应视为不可重试；业务方捕获后应改造消息或改用 Claim Check，而不是盲目重试。
- `MessageTooLargeException` 是运行时异常，在业务事务中抛出会触发回滚。**不要**在事务内捕获后继续提交业务数据，否则会出现“业务成功但消息缺失”的静默不一致。
- 数据库字段上限是 `TEXT` 的 **65,535 字节**，与默认应用上限一致。`max-payload-bytes` 不得超过该值；如需扩大，必须同步把列改为 `MEDIUMTEXT`、放宽启动校验，并重新评估 Broker、`max_allowed_packet` 和消费端限制。
- 同时还要满足 Broker 的 `max.message.bytes`、MySQL 的 `max_allowed_packet`（需覆盖 payload + headers + SQL 开销）以及消费端反序列化限制。组件上限应取所有限制中的最小值。
- 特别注意：`max_allowed_packet` 与 JDBC 驱动的 `maxAllowedPacket` 默认值可能只有 64KB 量级，按“64KB payload + 4KB headers + SQL 开销”计算，建议调到 1MB 以上（至少不低于 256KB），否则会被驱动或服务端提前拒绝。
- `headers` 默认上限 **4KB（4,096 字节）**，用于 `traceId`、`tenantId`、`schemaVersion` 等元数据；超出通常说明调用方把正文放错了位置，应改用 payload 或 Claim Check。`max-headers-bytes` 不得超过 `TEXT` 的 65,535 字节。
- 64KB 是标称值，实际可用上限是 65,535 字节；不要配置为 65,536。
- 超大消息建议使用 Claim Check 模式：消息体存对象存储，payload 中只放引用；该能力暂不内置。
- 边界测试至少覆盖：payload `actual == 65535` 通过、`actual == 65536` 拒绝、headers 4,096 字节通过 / 4,097 字节拒绝、多字节 UTF-8 字符按字节计数、超限后 Outbox 无新增记录。

## 7. 重试策略

采用指数退避 + 随机抖动策略：

```
重试间隔 = min(初始间隔 * 2^(重试次数-1), 最大间隔) + 随机抖动
```

默认配置：

| 参数 | 默认值 |
| --- | --- |
| 初始间隔 | 5 秒 |
| 最大间隔 | 300 秒 |
| 最大重试次数 | 16 次 |
| 随机抖动上限 | 1000 毫秒 |

时间轴示例（不含抖动）：5s → 10s → 20s → 40s → 80s → 160s → 300s → 300s（封顶）。

说明：

- 抖动用于打散重试波峰，避免大量消息在同一时刻集中重试造成数据库或 MQ 压力。
- 抖动上限配置为 0 时必须跳过随机数生成，避免 `ThreadLocalRandom.nextLong(0, 0)` 抛 `IllegalArgumentException`。
- `maxRetryCount = 16` 表示首次投递失败后最多再重试 16 次；总数约为 17 次投递。
- 不可重试错误不参与退避，直接进入 DEAD。

## 8. 详细代码设计

技术基线：Java 17、Spring Boot 4.x。

### 8.1 消息实体

```java
public enum OutboxStatus {
    INIT, FAILED, SENT, DEAD;

    public boolean isDispatchable() {
        return this == INIT || this == FAILED;
    }
}

@Data
public class OutboxMessage {
    private Long id;
    private String msgId;
    private String bizCode;
    private String topic;
    /** 透传给 Broker 的分区键，仅用于路由与消费分组，不承诺顺序。 */
    private String partitionKey;
    @ToString.Exclude
    private String payload;
    @ToString.Exclude
    private String headers;
    private OutboxStatus status;
    /** 软删除标记：true 表示已删除，不再参与投递。 */
    private boolean deleted;
    private int retryCount;
    private Instant nextRetryTime;
    private String ownerToken;
    private Instant ownerExpireAt;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant sentAt;
    private String lastError;
}
```

### 8.2 配置模型

```java
@Getter
@Setter
@ConfigurationProperties(prefix = "reliable-outbox")
public class ReliableOutboxProperties {
    private boolean enabled = true;
    private String tableName = "t_outbox";

    private int dispatchConcurrency = 8;
    private int dispatchQueueCapacity = 1000;

    private Duration ownerLease = Duration.ofSeconds(60);
    private Duration publishTimeout = Duration.ofSeconds(20);

    private Duration initScanGrace = Duration.ofSeconds(30);
    private Duration scanInterval = Duration.ofSeconds(30);
    private int scanBatchSize = 100;
    private int maxScanBatchesPerRun = 10;

    private int maxRetryCount = 16;
    private Duration initialRetryInterval = Duration.ofSeconds(5);
    private Duration maxRetryInterval = Duration.ofSeconds(300);
    private Duration retryJitter = Duration.ofMillis(1000);

    private Duration sentRetention = Duration.ofDays(7);
    private Duration cleanInterval = Duration.ofHours(1);
    private int cleanBatchSize = 1000;
    private int cleanMaxBatchesPerRun = 10;

    // 64KB 标称值；TEXT 实际上限为 65,535 字节
    private int maxPayloadBytes = 64 * 1024 - 1;
    // headers 是透传元数据，默认 4KB；TEXT 实际上限为 65,535 字节
    private int maxHeadersBytes = 4 * 1024;

    @PostConstruct
    void validate() {
        if (ownerLease.compareTo(publishTimeout) <= 0) {
            throw new IllegalStateException(
                    "reliable-outbox.owner-lease 必须大于 reliable-outbox.publish-timeout");
        }
        if (maxPayloadBytes <= 0 || maxPayloadBytes > 65535) {
            throw new IllegalStateException(
                    "reliable-outbox.max-payload-bytes 必须在 1..65535 之间（TEXT 上限）；" +
                    "如需更大消息，请同时修改 payload 列类型并移除该校验");
        }
        if (maxHeadersBytes <= 0 || maxHeadersBytes > 65535) {
            throw new IllegalStateException(
                    "reliable-outbox.max-headers-bytes 必须在 1..65535 之间（TEXT 上限）");
        }
    }
}
```

约定：

- `ownerLease` 必须大于 `publishTimeout`，启动时校验；否则发布尚未超时租约就可能被其他实例回收，导致重复投递。
- 默认 DDL 中 `payload` 与 `headers` 均为 `TEXT`，因此启动时同时校验 `maxPayloadBytes`、`maxHeadersBytes` ≤ 65535，避免应用放行后数据库插入失败。若改用 `MEDIUMTEXT`/`LONGTEXT`，需同步放宽对应校验。

### 8.3 OutboxRepository

```java
public interface OutboxRepository {
    /** 插入成功返回 true 并回填 id；msg_id 已存在返回 false。 */
    boolean insertIfAbsent(OutboxMessage message);

    OutboxMessage findById(Long id);

    OutboxMessage findByMsgId(String msgId);

    /** 不校验 next_retry_time；用于事务提交后的主动触发与人工回放。 */
    boolean tryAcquire(Long id, String ownerToken, Instant ownerExpireAt, Instant now);

    /** 同时校验 next_retry_time <= dueAt；用于扫描器和重试加速器，避免抢到刚被推迟的消息。 */
    boolean tryAcquireDue(Long id, String ownerToken, Instant ownerExpireAt, Instant dueAt);

    boolean releaseLease(Long id, String ownerToken, Instant now);

    boolean markSent(Long id, String ownerToken, Instant sentAt);

    /** dead=true 时 nextRetryTime 传 null。 */
    boolean markFailed(Long id, String ownerToken, String error,
                       Instant nextRetryTime, int retryCount,
                       boolean dead, Instant now);

    boolean resetForReplay(String msgId, Instant nextRetryTime, Instant now);

    /** 只返回 id，避免扫描时读取大字段 payload；按 next_retry_time、id 升序返回。 */
    List<Long> scanDispatchableIds(int limit, Instant now);

    List<Long> scanSentBefore(Instant before, int limit);

    int deleteSent(List<Long> ids);
}
```

### 8.4 JdbcOutboxRepository 关键实现

```java
public class JdbcOutboxRepository implements OutboxRepository {
    private static final Pattern TABLE_NAME =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;

    public JdbcOutboxRepository(JdbcTemplate jdbcTemplate, String tableName) {
        if (tableName == null || !TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException(
                    "非法表名，必须匹配 [A-Za-z_][A-Za-z0-9_]{0,63}: " + tableName);
        }
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    @Override
    public boolean insertIfAbsent(OutboxMessage message) {
        try {
            long id = insert(message);
            message.setId(id);
            return true;
        } catch (DuplicateKeyException e) {
            // 仅适用于 MySQL/InnoDB：单条语句失败不会使整个事务不可用。
            // PostgreSQL 请改用 INSERT ... ON CONFLICT DO NOTHING RETURNING id，
            // 因为 PG 中任何语句失败都会使当前事务进入 aborted 状态。
            return false;
        }
    }

    private long insert(OutboxMessage message) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(conn -> {
            PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + tableName + " (msg_id, biz_code, topic, partition_key, " +
                "payload, headers, status, retry_count, next_retry_time, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, 'INIT', 0, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, message.getMsgId());
            ps.setString(2, message.getBizCode());
            ps.setString(3, message.getTopic());
            ps.setString(4, message.getPartitionKey());
            ps.setString(5, message.getPayload());
            ps.setString(6, message.getHeaders());
            ps.setObject(7, toDbTime(message.getNextRetryTime()));
            ps.setObject(8, toDbTime(message.getCreatedAt()));
            ps.setObject(9, toDbTime(message.getUpdatedAt()));
            return ps;
        }, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public OutboxMessage findById(Long id) {
        List<OutboxMessage> list = jdbcTemplate.query(
            "SELECT * FROM " + tableName + " WHERE id = ?", outboxRowMapper(), id);
        return list.isEmpty() ? null : list.get(0);
    }

    @Override
    public OutboxMessage findByMsgId(String msgId) {
        List<OutboxMessage> list = jdbcTemplate.query(
            "SELECT * FROM " + tableName + " WHERE msg_id = ?", outboxRowMapper(), msgId);
        return list.isEmpty() ? null : list.get(0);
    }

    private RowMapper<OutboxMessage> outboxRowMapper() {
        return (rs, rowNum) -> {
            OutboxMessage message = new OutboxMessage();
            message.setId(rs.getLong("id"));
            message.setMsgId(rs.getString("msg_id"));
            message.setBizCode(rs.getString("biz_code"));
            message.setTopic(rs.getString("topic"));
            message.setPartitionKey(rs.getString("partition_key"));
            message.setPayload(rs.getString("payload"));
            message.setHeaders(rs.getString("headers"));
            message.setStatus(OutboxStatus.valueOf(rs.getString("status")));
            message.setDeleted(rs.getBoolean("deleted"));
            message.setRetryCount(rs.getInt("retry_count"));
            message.setNextRetryTime(toInstant(rs, "next_retry_time"));
            message.setOwnerToken(rs.getString("owner_token"));
            message.setOwnerExpireAt(toInstant(rs, "owner_expire_at"));
            message.setCreatedAt(toInstant(rs, "created_at"));
            message.setUpdatedAt(toInstant(rs, "updated_at"));
            message.setSentAt(toInstant(rs, "sent_at"));
            message.setLastError(rs.getString("last_error"));
            return message;
        };
    }

    private static LocalDateTime toDbTime(Instant instant) {
        return instant == null ? null
                : LocalDateTime.ofInstant(instant.truncatedTo(ChronoUnit.MILLIS), ZoneOffset.UTC);
    }

    private static Instant toInstant(ResultSet rs, String column) throws SQLException {
        LocalDateTime value = rs.getObject(column, LocalDateTime.class);
        return value == null ? null : value.toInstant(ZoneOffset.UTC);
    }

    @Override
    public boolean tryAcquire(Long id, String ownerToken, Instant ownerExpireAt, Instant now) {
        int rows = jdbcTemplate.update(
            "UPDATE " + tableName + " SET owner_token = ?, owner_expire_at = ?, updated_at = ? " +
            "WHERE id = ? AND status IN ('INIT', 'FAILED') AND deleted = 0 " +
            "AND (owner_token IS NULL OR owner_expire_at < ?)",
            ownerToken, toDbTime(ownerExpireAt), toDbTime(now),
            id, toDbTime(now));
        return rows == 1;
    }

    @Override
    public boolean tryAcquireDue(Long id, String ownerToken, Instant ownerExpireAt, Instant dueAt) {
        int rows = jdbcTemplate.update(
            "UPDATE " + tableName + " SET owner_token = ?, owner_expire_at = ?, updated_at = ? " +
            "WHERE id = ? AND status IN ('INIT', 'FAILED') AND deleted = 0 " +
            "AND next_retry_time IS NOT NULL AND next_retry_time <= ? " +
            "AND (owner_token IS NULL OR owner_expire_at < ?)",
            ownerToken, toDbTime(ownerExpireAt), toDbTime(dueAt),
            id, toDbTime(dueAt), toDbTime(dueAt));
        return rows == 1;
    }

    @Override
    public boolean releaseLease(Long id, String ownerToken, Instant now) {
        int rows = jdbcTemplate.update(
            "UPDATE " + tableName + " SET owner_token = NULL, owner_expire_at = NULL, updated_at = ? " +
            "WHERE id = ? AND owner_token = ?",
            toDbTime(now), id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean markSent(Long id, String ownerToken, Instant sentAt) {
        int rows = jdbcTemplate.update(
            "UPDATE " + tableName + " SET status = 'SENT', owner_token = NULL, owner_expire_at = NULL, " +
            "sent_at = ?, next_retry_time = NULL, last_error = NULL, updated_at = ? " +
            "WHERE id = ? AND owner_token = ? AND status IN ('INIT','FAILED')",
            toDbTime(sentAt), toDbTime(sentAt), id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean markFailed(Long id, String ownerToken, String error,
                              Instant nextRetryTime, int retryCount,
                              boolean dead, Instant now) {
        if (dead) {
            int rows = jdbcTemplate.update(
                "UPDATE " + tableName + " SET status = 'DEAD', owner_token = NULL, owner_expire_at = NULL, " +
                "retry_count = ?, next_retry_time = NULL, last_error = ?, updated_at = ? " +
                "WHERE id = ? AND owner_token = ? AND status IN ('INIT','FAILED')",
                retryCount, error, toDbTime(now), id, ownerToken);
            return rows == 1;
        }
        int rows = jdbcTemplate.update(
            "UPDATE " + tableName + " SET status = 'FAILED', owner_token = NULL, owner_expire_at = NULL, " +
            "retry_count = ?, next_retry_time = ?, last_error = ?, updated_at = ? " +
            "WHERE id = ? AND owner_token = ? AND status IN ('INIT','FAILED')",
            retryCount, toDbTime(nextRetryTime), error, toDbTime(now), id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean resetForReplay(String msgId, Instant nextRetryTime, Instant now) {
        int rows = jdbcTemplate.update(
            "UPDATE " + tableName + " SET status = 'INIT', retry_count = 0, " +
            "next_retry_time = ?, " +
            "owner_token = NULL, owner_expire_at = NULL, last_error = NULL, updated_at = ? " +
            "WHERE msg_id = ? AND status = 'DEAD' AND deleted = 0",
            toDbTime(nextRetryTime), toDbTime(now), msgId);
        return rows == 1;
    }

    @Override
    public List<Long> scanDispatchableIds(int limit, Instant now) {
        return jdbcTemplate.queryForList(
            "SELECT id FROM " + tableName + " " +
            "WHERE status IN ('INIT', 'FAILED') AND deleted = 0 " +
            "AND next_retry_time IS NOT NULL " +
            "AND next_retry_time <= ? " +
            "AND (owner_token IS NULL OR owner_expire_at < ?) " +
            "ORDER BY next_retry_time, id LIMIT ?",
            Long.class, toDbTime(now), toDbTime(now), limit);
    }

    @Override
    public List<Long> scanSentBefore(Instant before, int limit) {
        return jdbcTemplate.queryForList(
            "SELECT id FROM " + tableName + " WHERE status = 'SENT' AND sent_at < ? " +
            "ORDER BY sent_at LIMIT ?",
            Long.class, toDbTime(before), limit);
    }

    @Override
    public int deleteSent(List<Long> ids) {
        if (ids.isEmpty()) return 0;
        String inSql = String.join(",", Collections.nCopies(ids.size(), "?"));
        return jdbcTemplate.update(
            "DELETE FROM " + tableName + " WHERE id IN (" + inSql + ") AND status = 'SENT'",
            ids.toArray());
    }
}
```

说明：

- 表名来自配置，必须经过白名单校验后再拼接，避免 SQL 注入。
- 时间字段统一用 `DATETIME(3)` 存取（UTC，毫秒精度），代码层使用 `Instant`；绑定参数统一通过 `toDbTime()` 转为 UTC 的 `LocalDateTime`，读取时反向转换，不依赖 JDBC 会话时区。
- 不建议在业务事务内 `catch DuplicateKeyException` 后继续执行 PostgreSQL 事务；跨库适配应放在 Repository SPI 中实现。
- 扫描按 `(next_retry_time, id)` 升序返回，只为稳定取数和公平性，不构成投递顺序保证。

### 8.5 TransactionExecutor

```java
public interface TransactionExecutor {
    /** 加入已存在的 Outbox 数据源事务；不存在时抛出异常。 */
    <T> T executeInExistingTransaction(Supplier<T> action);

    /** 挂起当前事务并创建独立事务。 */
    <T> T executeInNewTransaction(Supplier<T> action);

    /** 必须在上述事务执行期间调用。 */
    void afterCommit(Runnable action);
}

public class SpringTransactionExecutor implements TransactionExecutor {
    private static final Logger log = LoggerFactory.getLogger(SpringTransactionExecutor.class);

    private final TransactionTemplate mandatory;
    private final TransactionTemplate requiresNew;

    public SpringTransactionExecutor(PlatformTransactionManager transactionManager) {
        this.mandatory = new TransactionTemplate(transactionManager);
        this.mandatory.setPropagationBehavior(TransactionDefinition.PROPAGATION_MANDATORY);

        this.requiresNew = new TransactionTemplate(transactionManager);
        this.requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public <T> T executeInExistingTransaction(Supplier<T> action) {
        return mandatory.execute(status -> action.get());
    }

    @Override
    public <T> T executeInNewTransaction(Supplier<T> action) {
        return requiresNew.execute(status -> action.get());
    }

    @Override
    public void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()
                || !TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("当前没有可注册 afterCommit 的活动事务");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    action.run();
                } catch (Throwable ex) {
                    // 当前事务已提交，异常不得再向调用方传播
                    log.error("reliable-outbox afterCommit action failed", ex);
                }
            }
        });
    }
}
```

约定：

- `sendInTransaction` 通过 `PROPAGATION_MANDATORY` 验证指定事务管理器上确实存在事务，而不是只检查线程上是否存在任意 Spring 事务。
- `sendStandalone` 通过 `PROPAGATION_REQUIRES_NEW` 创建独立事务；若外层已有事务，会先挂起外层事务。
- `TransactionExecutor` 使用的事务管理器必须覆盖 `OutboxRepository` 使用的数据源。多事务管理器场景由业务方显式提供对应的 `TransactionExecutor` bean，禁止自动猜测。
- `afterCommit` 只负责注册回调，不提供“无事务时立即执行”的降级行为；两个入口都必须先建立明确的事务边界。

### 8.6 MessageClient 与实现

```java
public interface MessageClient {
    /** 必须加入已存在的业务事务。 */
    String sendInTransaction(OutboxMessage message);

    /** 使用独立的 REQUIRES_NEW 事务。 */
    String sendStandalone(OutboxMessage message);
}

public class DefaultMessageClient implements MessageClient {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageClient.class);

    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;
    private final TransactionExecutor transactionExecutor;
    private final ExecutorService dispatchExecutor;
    private final ReliableOutboxProperties properties;

    public DefaultMessageClient(OutboxRepository repository,
                                MessageDispatcher dispatcher,
                                TransactionExecutor transactionExecutor,
                                ExecutorService dispatchExecutor,
                                ReliableOutboxProperties properties) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.transactionExecutor = transactionExecutor;
        this.dispatchExecutor = dispatchExecutor;
        this.properties = properties;
    }

    private enum SendMode {
        IN_TRANSACTION,
        STANDALONE
    }

    @Override
    public String sendInTransaction(OutboxMessage message) {
        return doSend(message, SendMode.IN_TRANSACTION);
    }

    @Override
    public String sendStandalone(OutboxMessage message) {
        return doSend(message, SendMode.STANDALONE);
    }

    private String doSend(OutboxMessage message, SendMode mode) {
        validate(message);

        Supplier<String> persistAction = () -> {
            prepare(message);
            return persistAndRegisterDispatch(message);
        };

        return switch (mode) {
            case IN_TRANSACTION ->
                    transactionExecutor.executeInExistingTransaction(persistAction);
            case STANDALONE ->
                    transactionExecutor.executeInNewTransaction(persistAction);
        };
    }

    private String persistAndRegisterDispatch(OutboxMessage message) {
        boolean inserted = repository.insertIfAbsent(message);
        if (!inserted) {
            OutboxMessage existing = repository.findByMsgId(message.getMsgId());
            if (existing != null
                    && (!Objects.equals(existing.getPayload(), message.getPayload())
                        || !Objects.equals(existing.getTopic(), message.getTopic())
                        || !Objects.equals(existing.getPartitionKey(), message.getPartitionKey()))) {
                log.warn("duplicate msgId with different content, msgId={}", message.getMsgId());
            }
            return message.getMsgId();
        }

        transactionExecutor.afterCommit(() -> submitDispatch(message.getId()));
        return message.getMsgId();
    }

    private void prepare(OutboxMessage message) {
        Instant now = Instant.now();
        if (message.getMsgId() == null) {
            message.setMsgId(UUID.randomUUID().toString().replace("-", ""));
        }
        message.setStatus(OutboxStatus.INIT);
        message.setRetryCount(0);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        message.setNextRetryTime(now.plus(properties.getInitScanGrace()));
    }

    private void submitDispatch(Long id) {
        try {
            dispatchExecutor.execute(() -> {
                try {
                    dispatcher.dispatchById(id);
                } catch (Throwable ex) {
                    log.error("async dispatch failed, id={}", id, ex);
                }
            });
        } catch (RejectedExecutionException ex) {
            // 记录已持久化，扫描器会在 next_retry_time 到期后兜底
            log.warn("dispatch queue full, id={} will be picked up by scanner", id);
        }
    }

    private void validate(OutboxMessage message) {
        Objects.requireNonNull(message, "message");
        requireText(message.getBizCode(), "bizCode");
        requireText(message.getTopic(), "topic");
        if (message.getPayload() == null) {
            throw new IllegalArgumentException("payload 不能为空");
        }
        if (message.getBizCode().length() > 64) {
            throw new IllegalArgumentException("bizCode 长度不能超过 64");
        }
        if (message.getTopic().length() > 255) {
            throw new IllegalArgumentException("topic 长度不能超过 255");
        }
        if (message.getPartitionKey() != null && message.getPartitionKey().length() > 255) {
            throw new IllegalArgumentException("partitionKey 长度不能超过 255");
        }
        if (message.getMsgId() != null
                && !message.getMsgId().matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException("msgId 必须为 1~64 位 ASCII 字母、数字、下划线或短横线");
        }
        int payloadBytes = message.getPayload().getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > properties.getMaxPayloadBytes()) {
            throw new MessageTooLargeException(
                    "payload", payloadBytes, properties.getMaxPayloadBytes());
        }
        if (message.getHeaders() != null) {
            int headerBytes = message.getHeaders().getBytes(StandardCharsets.UTF_8).length;
            if (headerBytes > properties.getMaxHeadersBytes()) {
                throw new MessageTooLargeException(
                        "headers", headerBytes, properties.getMaxHeadersBytes());
            }
        }
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " 不能为空");
        }
    }
}

public class MessageTooLargeException extends IllegalArgumentException {
    private final String field;
    private final int actualBytes;
    private final int maxBytes;

    public MessageTooLargeException(String field, int actualBytes, int maxBytes) {
        super(field + " 超过上限: actual=" + actualBytes + " bytes, max=" + maxBytes + " bytes");
        this.field = field;
        this.actualBytes = actualBytes;
        this.maxBytes = maxBytes;
    }

    public String getField() { return field; }
    public int getActualBytes() { return actualBytes; }
    public int getMaxBytes() { return maxBytes; }
}


消息体大小校验说明：

- 校验位于序列化之后、`insertIfAbsent` 之前，超限时不会产生任何数据库写入，也不会进入重试。
- `MessageTooLargeException` 继承 `IllegalArgumentException`，调用方可以按需精确捕获；它属于参数错误，不应被 `MessagePublisher` 当作可重试的发布失败。
- 若接入 `MessageConverter`，必须在转换完成后调用同一校验逻辑，禁止只校验原始对象。

分区键示例：

```java
OutboxMessage message = new OutboxMessage();
message.setBizCode("order");
message.setTopic("order-events");
message.setPartitionKey(orderId);   // 仅用于 Broker 分区路由与消费分组
message.setPayload(payloadJson);

// 与业务数据原子提交：调用时必须已有与 Outbox 数据源对应的事务
messageClient.sendInTransaction(message);

// 独立可靠发送：始终使用 REQUIRES_NEW 短事务
// messageClient.sendStandalone(message);
```

> `partition_key` 只影响 Broker 的分区/队列选择，**不代表组件提供顺序保证**。需要顺序的业务必须在生产端串行提交，并在消费端按 key 串行消费或使用版本号收敛。

### 8.7 MessageDispatcher

```java
public class MessageDispatcher {
    private static final Logger log = LoggerFactory.getLogger(MessageDispatcher.class);

    private final OutboxRepository repository;
    private final MessagePublisher publisher;
    private final RetryPolicy retryPolicy;
    private final RetryScheduler retryScheduler;
    private final DeadLetterHandler deadLetterHandler;
    private final MessageListener listener;
    private final Duration ownerLease;
    private final Duration publishTimeout;

    public MessageDispatcher(OutboxRepository repository,
                             MessagePublisher publisher,
                             RetryPolicy retryPolicy,
                             RetryScheduler retryScheduler,
                             DeadLetterHandler deadLetterHandler,
                             MessageListener listener,
                             Duration ownerLease,
                             Duration publishTimeout) {
        this.repository = repository;
        this.publisher = publisher;
        this.retryPolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
        this.deadLetterHandler = deadLetterHandler;
        this.listener = listener;
        this.ownerLease = ownerLease;
        this.publishTimeout = publishTimeout;
    }

    public void dispatchById(Long id) {
        Instant now = Instant.now();
        String ownerToken = UUID.randomUUID().toString();
        if (!repository.tryAcquire(id, ownerToken, now.plus(ownerLease), now)) {
            return;
        }
        dispatchAcquired(id, ownerToken);
    }

    public void dispatchDue(Long id) {
        Instant now = Instant.now();
        String ownerToken = UUID.randomUUID().toString();
        if (!repository.tryAcquireDue(id, ownerToken, now.plus(ownerLease), now)) {
            return;
        }
        dispatchAcquired(id, ownerToken);
    }

    /** 调用方必须已持有该消息的租约。 */
    public void dispatchAcquired(Long id, String ownerToken) {
        OutboxMessage fresh = repository.findById(id);
        if (fresh == null) {
            log.warn("outbox message not found, id={}", id);
            return;
        }
        if (fresh.isDeleted()) {
            // 抢占后被软删除：不再发布，租约到期后自然失效
            log.debug("outbox message deleted, skip, id={}, msgId={}", id, fresh.getMsgId());
            return;
        }
        Instant now = Instant.now();
        if (!ownerToken.equals(fresh.getOwnerToken())) {
            // 任务在队列中等待过久，租约已被其他实例接管
            log.warn("lease lost before publish, skip, id={}, msgId={}", id, fresh.getMsgId());
            safeNotify(() -> listener.onLeaseLost(fresh, "lease-lost-before-publish"));
            return;
        }
        if (fresh.getOwnerExpireAt() == null
                || !fresh.getOwnerExpireAt().isAfter(now.plus(publishTimeout))) {
            // 剩余租约不足以覆盖一次完整发布：主动归还，让扫描器尽快重新抢占
            log.warn("lease insufficient before publish, release, id={}, msgId={}",
                     id, fresh.getMsgId());
            repository.releaseLease(id, ownerToken, now);
            return;
        }
        try {
            publisher.publish(fresh);
            Instant sentAt = Instant.now();
            if (!repository.markSent(id, ownerToken, sentAt)) {
                log.warn("markSent failed, lease may be lost, duplicate delivery possible, msgId={}",
                         fresh.getMsgId());
                safeNotify(() -> listener.onLeaseLost(fresh, "mark-sent-failed"));
                return;
            }
            fresh.setStatus(OutboxStatus.SENT);
            fresh.setSentAt(sentAt);
            safeNotify(() -> listener.onSent(fresh));
        } catch (NonRetryablePublishException ex) {
            fail(fresh, ownerToken, ex, true);
        } catch (Exception ex) {
            fail(fresh, ownerToken, ex, false);
        }
    }

    public void retry(Long id) {
        OutboxMessage message = repository.findById(id);
        if (message == null || !message.getStatus().isDispatchable()) {
            return;
        }
        dispatchDue(id);
    }

    private void fail(OutboxMessage message, String ownerToken, Exception error, boolean permanent) {
        int retryCount = message.getRetryCount() + 1;
        boolean dead = permanent || retryPolicy.isDead(retryCount);
        Instant nextRetryTime = dead ? null : retryPolicy.nextRetryTime(retryCount);
        String errorMessage = truncate(error.getMessage());

        boolean updated = repository.markFailed(message.getId(), ownerToken,
                errorMessage, nextRetryTime, retryCount, dead, Instant.now());
        if (!updated) {
            log.warn("markFailed ignored, lease lost, msgId={}", message.getMsgId());
            return;
        }

        // 同步内存状态后再通知监听器，避免指标/告警读到旧值
        message.setRetryCount(retryCount);
        message.setLastError(errorMessage);
        message.setStatus(dead ? OutboxStatus.DEAD : OutboxStatus.FAILED);
        if (!dead) {
            message.setNextRetryTime(nextRetryTime);
        }

        if (dead) {
            deadLetterHandler.onDead(message);
            return;
        }

        safeNotify(() -> listener.onFailed(message, retryCount, error));
        long delayMillis = Math.max(0,
                Duration.between(Instant.now(), nextRetryTime).toMillis());
        retryScheduler.schedule(message.getId(), delayMillis);
    }

    private void safeNotify(Runnable action) {
        try {
            action.run();
        } catch (Throwable ex) {
            log.warn("message listener failed", ex);
        }
    }

    private String truncate(String error) {
        if (error == null) return null;
        return error.length() > 1000 ? error.substring(0, 1000) : error;
    }
}
```

### 8.8 RetryPolicy

```java
public class RetryPolicy {
    private final int maxRetryCount;
    private final Duration initialInterval;
    private final Duration maxInterval;
    private final long maxJitterMillis;

    public RetryPolicy(int maxRetryCount, Duration initialInterval,
                       Duration maxInterval, Duration maxJitter) {
        this.maxRetryCount = maxRetryCount;
        this.initialInterval = initialInterval;
        this.maxInterval = maxInterval;
        this.maxJitterMillis = maxJitter.toMillis();
    }

    public boolean isDead(int retryCount) {
        return retryCount >= maxRetryCount;
    }

    public Instant nextRetryTime(int retryCount) {
        int shift = Math.min(Math.max(0, retryCount - 1), 20);
        long baseSeconds = initialInterval.getSeconds() * (1L << shift);
        long effectiveSeconds = Math.min(baseSeconds, maxInterval.getSeconds());
        long jitterMillis = maxJitterMillis > 0
                ? ThreadLocalRandom.current().nextLong(maxJitterMillis)
                : 0L;
        return Instant.now()
                .plusSeconds(effectiveSeconds)
                .plusMillis(jitterMillis);
    }
}
```

### 8.9 RetryScheduler

```java
public interface RetryScheduler {
    void schedule(Long messageId, long delayMillis);
}

public class DefaultRetryScheduler implements RetryScheduler {
    private static final Logger log = LoggerFactory.getLogger(DefaultRetryScheduler.class);

    private final ObjectProvider<MessageDispatcher> dispatcherProvider;
    private final Executor dispatchExecutor;
    private final ScheduledExecutorService scheduler;

    public DefaultRetryScheduler(ObjectProvider<MessageDispatcher> dispatcherProvider,
                                 Executor dispatchExecutor,
                                 ScheduledExecutorService scheduler) {
        this.dispatcherProvider = dispatcherProvider;
        this.dispatchExecutor = dispatchExecutor;
        this.scheduler = scheduler;
    }

    @Override
    public void schedule(Long messageId, long delayMillis) {
        scheduler.schedule(() -> {
            try {
                dispatchExecutor.execute(() -> dispatcherProvider.getObject().retry(messageId));
            } catch (RejectedExecutionException ex) {
                // 扫描器兜底
                log.warn("retry dispatch rejected, id={} will be retried by scanner", messageId);
            }
        }, Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
    }
}
```

使用 `ObjectProvider` 是为了打破 `MessageDispatcher ↔ RetryScheduler` 的构造器循环依赖；延迟任务只提交到投递线程池，避免单线程定时器被 MQ 调用阻塞。

### 8.10 MessageScanner

```java
public class MessageScanner {
    private static final Logger log = LoggerFactory.getLogger(MessageScanner.class);

    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;
    private final ExecutorService dispatchExecutor;
    private final ScheduledExecutorService scheduler;
    private final ReliableOutboxProperties properties;

    public MessageScanner(OutboxRepository repository,
                          MessageDispatcher dispatcher,
                          ExecutorService dispatchExecutor,
                          ScheduledExecutorService scheduler,
                          ReliableOutboxProperties properties) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.dispatchExecutor = dispatchExecutor;
        this.scheduler = scheduler;
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        long interval = properties.getScanInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::scanSafely, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void scanSafely() {
        try {
            scan();
        } catch (Throwable ex) {
            log.error("reliable-outbox scan failed", ex);
        }
    }

    public void scan() {
        int batchSize = properties.getScanBatchSize();
        for (int round = 0; round < properties.getMaxScanBatchesPerRun(); round++) {
            List<Long> ids = repository.scanDispatchableIds(batchSize, Instant.now());
            if (ids.isEmpty()) {
                return;
            }
            for (Long id : ids) {
                if (!hasExecutorCapacity()) {
                    return;
                }
                Instant now = Instant.now();
                String ownerToken = UUID.randomUUID().toString();
                if (!repository.tryAcquireDue(id, ownerToken,
                        now.plus(properties.getOwnerLease()), now)) {
                    continue;
                }
                try {
                    dispatchExecutor.execute(() -> dispatcher.dispatchAcquired(id, ownerToken));
                } catch (RejectedExecutionException ex) {
                    log.warn("dispatch queue full, release lease, id={}", id);
                    repository.releaseLease(id, ownerToken, Instant.now());
                }
            }
            if (ids.size() < batchSize) {
                return;
            }
        }
    }

    private boolean hasExecutorCapacity() {
        return !(dispatchExecutor instanceof ThreadPoolExecutor executor)
                || executor.getQueue().remainingCapacity() > 0;
    }
}
```

扫描线程只做“查询 ID + 抢占 + 提交”，MQ 调用与完整消息读取都发生在投递线程池中；多个实例同时扫描是安全的，且同一批内已抢占的行不会在下一轮被重复查询到。

扫描器在抢占前检查投递队列剩余容量，避免 MQ 变慢时把大量已抢占任务堆在队列中、等待超过租约后被 skip，造成重试放大。

### 8.11 OutboxCleaner

```java
public class OutboxCleaner {
    private static final Logger log = LoggerFactory.getLogger(OutboxCleaner.class);

    private final OutboxRepository repository;
    private final ScheduledExecutorService scheduler;
    private final ReliableOutboxProperties properties;

    public OutboxCleaner(OutboxRepository repository,
                         ScheduledExecutorService scheduler,
                         ReliableOutboxProperties properties) {
        this.repository = repository;
        this.scheduler = scheduler;
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        long interval = properties.getCleanInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::cleanSafely, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void cleanSafely() {
        try {
            clean();
        } catch (Throwable ex) {
            log.error("reliable-outbox cleanup failed", ex);
        }
    }

    public void clean() {
        Instant before = Instant.now().minus(properties.getSentRetention());
        int batchSize = properties.getCleanBatchSize();
        int total = 0;
        for (int round = 0; round < properties.getCleanMaxBatchesPerRun(); round++) {
            List<Long> ids = repository.scanSentBefore(before, batchSize);
            if (ids.isEmpty()) {
                break;
            }
            total += repository.deleteSent(ids);
            if (ids.size() < batchSize) {
                break;
            }
        }
        if (total > 0) {
            log.info("reliable-outbox cleaner deleted {} SENT messages", total);
        }
    }
}
```

### 8.12 死信处理与回放

```java
public interface MessageListener {
    default void onSent(OutboxMessage message) {
    }

    default void onFailed(OutboxMessage message, int retryCount, Throwable error) {
    }

    default void onDead(OutboxMessage message) {
    }

    /** 租约丢失、markSent 失败等异常路径，用于重复投递风险指标。 */
    default void onLeaseLost(OutboxMessage message, String reason) {
    }
}

public class LoggingMessageListener implements MessageListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingMessageListener.class);

    @Override
    public void onSent(OutboxMessage message) {
        log.debug("outbox message sent, msgId={}", message.getMsgId());
    }

    @Override
    public void onFailed(OutboxMessage message, int retryCount, Throwable error) {
        log.warn("outbox message failed, msgId={}, retryCount={}", message.getMsgId(), retryCount, error);
    }

    @Override
    public void onDead(OutboxMessage message) {
        log.error("outbox message DEAD, msgId={}, bizCode={}, topic={}, retryCount={}, lastError={}",
                message.getMsgId(), message.getBizCode(), message.getTopic(),
                message.getRetryCount(), message.getLastError());
    }

    @Override
    public void onLeaseLost(OutboxMessage message, String reason) {
        log.warn("outbox lease lost, msgId={}, reason={}", message.getMsgId(), reason);
    }
}

public class DeadLetterHandler {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterHandler.class);

    private final MessageListener listener;

    public DeadLetterHandler(MessageListener listener) {
        this.listener = listener;
    }

    public void onDead(OutboxMessage message) {
        try {
            listener.onDead(message);
        } catch (Throwable ex) {
            log.error("dead letter listener failed, msgId={}", message.getMsgId(), ex);
        }
    }
}

public class DeadLetterReplayer {
    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;

    public DeadLetterReplayer(OutboxRepository repository,
                              MessageDispatcher dispatcher) {
        this.repository = repository;
        this.dispatcher = dispatcher;
    }

    public boolean replay(String msgId) {
        Instant now = Instant.now();
        boolean reset = repository.resetForReplay(msgId, now, now);
        if (!reset) {
            return false;
        }
        OutboxMessage message = repository.findByMsgId(msgId);
        if (message != null) {
            dispatcher.dispatchById(message.getId());
        }
        return true;
    }
}
```

说明：`DeadLetterHandler` 不再依赖 `MessageDispatcher`，回放职责拆到 `DeadLetterReplayer`，从结构上消除 `Dispatcher ↔ DeadLetterHandler` 的循环依赖。

### 8.13 SPI 接口

```java
public interface MessagePublisher {
    /**
     * 正常返回代表 Broker 已确认。
     * 可重试失败抛出 PublishException；不可重试失败抛出 NonRetryablePublishException。
     */
    void publish(OutboxMessage message) throws PublishException;
}

public class PublishException extends RuntimeException {
    public PublishException(String message, Throwable cause) {
        super(message, cause);
    }
}

public class NonRetryablePublishException extends PublishException {
    public NonRetryablePublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

### 8.14 自动装配

```java
@AutoConfiguration
@EnableConfigurationProperties(ReliableOutboxProperties.class)
@ConditionalOnProperty(prefix = "reliable-outbox", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ReliableOutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxRepository outboxRepository(DataSource dataSource, ReliableOutboxProperties properties) {
        return new JdbcOutboxRepository(new JdbcTemplate(dataSource), properties.getTableName());
    }

    @Bean(name = "reliableOutboxDispatchExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "reliableOutboxDispatchExecutor")
    public ExecutorService reliableOutboxDispatchExecutor(ReliableOutboxProperties properties) {
        return new ThreadPoolExecutor(
                properties.getDispatchConcurrency(),
                properties.getDispatchConcurrency(),
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getDispatchQueueCapacity()),
                namedThreadFactory("reliable-outbox-dispatch-"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean(name = "reliableOutboxScheduler", destroyMethod = "shutdownNow")
    @ConditionalOnMissingBean(name = "reliableOutboxScheduler")
    public ScheduledExecutorService reliableOutboxScheduler() {
        // 扫描、清理、重试加速各占一个长任务线程
        return Executors.newScheduledThreadPool(3, namedThreadFactory("reliable-outbox-scheduler-"));
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(PlatformTransactionManager.class)
    public TransactionExecutor transactionExecutor(
            PlatformTransactionManager transactionManager) {
        return new SpringTransactionExecutor(transactionManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy(ReliableOutboxProperties properties) {
        return new RetryPolicy(properties.getMaxRetryCount(),
                properties.getInitialRetryInterval(),
                properties.getMaxRetryInterval(),
                properties.getRetryJitter());
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageListener messageListener() {
        return new LoggingMessageListener();
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterHandler deadLetterHandler(MessageListener listener) {
        return new DeadLetterHandler(listener);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageDispatcher messageDispatcher(OutboxRepository repository,
                                               MessagePublisher publisher,
                                               RetryPolicy retryPolicy,
                                               RetryScheduler retryScheduler,
                                               DeadLetterHandler deadLetterHandler,
                                               MessageListener listener,
                                               ReliableOutboxProperties properties) {
        return new MessageDispatcher(repository, publisher, retryPolicy, retryScheduler,
                deadLetterHandler, listener,
                properties.getOwnerLease(), properties.getPublishTimeout());
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryScheduler retryScheduler(ObjectProvider<MessageDispatcher> dispatcherProvider,
                                         @Qualifier("reliableOutboxDispatchExecutor") ExecutorService executor,
                                         @Qualifier("reliableOutboxScheduler") ScheduledExecutorService scheduler) {
        return new DefaultRetryScheduler(dispatcherProvider, executor, scheduler);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageClient messageClient(OutboxRepository repository,
                                       MessageDispatcher dispatcher,
                                       TransactionExecutor transactionExecutor,
                                       @Qualifier("reliableOutboxDispatchExecutor") ExecutorService executor,
                                       ReliableOutboxProperties properties) {
        return new DefaultMessageClient(repository, dispatcher, transactionExecutor,
                executor, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageScanner messageScanner(OutboxRepository repository,
                                         MessageDispatcher dispatcher,
                                         @Qualifier("reliableOutboxDispatchExecutor") ExecutorService executor,
                                         @Qualifier("reliableOutboxScheduler") ScheduledExecutorService scheduler,
                                         ReliableOutboxProperties properties) {
        return new MessageScanner(repository, dispatcher, executor, scheduler, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxCleaner outboxCleaner(OutboxRepository repository,
                                       @Qualifier("reliableOutboxScheduler") ScheduledExecutorService scheduler,
                                       ReliableOutboxProperties properties) {
        return new OutboxCleaner(repository, scheduler, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterReplayer deadLetterReplayer(OutboxRepository repository,
                                                 MessageDispatcher dispatcher) {
        return new DeadLetterReplayer(repository, dispatcher);
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
```

关键点：

- 所有 bean 都提供 `@ConditionalOnMissingBean`，业务方可覆盖 `MessagePublisher`、`OutboxRepository`、`MessageListener` 等实现。
- 仅存在单个 `PlatformTransactionManager` 候选时自动创建 `SpringTransactionExecutor`。多事务管理器场景必须显式提供与 Outbox 数据源匹配的 `TransactionExecutor`，避免把 `sendInTransaction` 绑定到错误事务。
- 不再在 starter 中强制 `@EnableScheduling`，后台任务由 reliable-outbox 自有的 `ScheduledExecutorService` 驱动，避免与业务调度线程互相影响。
- 循环依赖已消除：`RetryScheduler` 通过 `ObjectProvider` 延迟获取 `MessageDispatcher`；死信回放拆为独立类。
- 该自动配置需通过 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 注册。

## 9. SPI 扩展点

| 扩展点 | 职责 | 常见实现 |
| --- | --- | --- |
| OutboxRepository | Outbox 表的读写与租约抢占，适配不同数据库方言与表结构 | MySQL / PostgreSQL / Oracle |
| TransactionExecutor | 执行 MANDATORY / REQUIRES_NEW 事务并注册提交回调，适配事务框架 | Spring Transaction / 其他事务框架 |
| MessagePublisher | 发送消息到具体 MQ，必须返回即代表 Broker 已确认 | Kafka / RocketMQ / RabbitMQ / Pulsar |
| RetryScheduler | 延迟触发单条消息重试的加速器（非可靠定时器） | Spring Task / Quartz / XXL-Job |
| MessageListener | 投递成功、失败、死信事件，用于指标与告警 | Micrometer / 钉钉 / 企微 / 邮件 |
| MessageConverter | 消息体序列化与反序列化（可选） | JSON / Protobuf |
| LockManager | 扫描器选主/减少空扫（可选，不参与正确性保证） | ShedLock / 数据库锁 |

说明：

- `MessagePublisher` 是**必须由使用方提供**的 bean，reliable-outbox 不内置默认实现；缺失时应用启动失败并给出明确错误。
- `MessagePublisher` 必须在内部处理发送超时，超时时间应读取 `reliable-outbox.publish-timeout`（并小于 `ownerLease`），保证与租约检查一致。
- 多 MQ 适配时，消息信封（topic、partition key、headers、msgId）由 reliable-outbox 统一提供，各实现负责映射到具体 MQ 属性。
- `MessagePublisher` 应将 `partition_key` 映射到 Broker 的分区键/队列选择器，但**不得据此声称投递有序**；顺序保证不属于组件契约，需由接入方与消费端自行实现。
- 如果引入 `MessageConverter`，应在 `payload` 落库前完成序列化，保证 Outbox 中存储的是可投递的最终形态。
- `MessageListener` 的回调可能从投递线程池并发触发，实现方需保证线程安全，且回调中不应执行耗时操作。
- 多数据源场景下，`OutboxRepository` 的自动装配需要显式指定业务库的 `DataSource`（通过 `@Qualifier` 或自定义 bean 覆盖）。

## 10. 部署与运维

### 10.1 推荐部署结构

- reliable-outbox 以独立发布的库依赖接入业务服务，并随业务服务进程一同部署；多实例运行时通过 `owner_token` 租约保证并发安全。
- Outbox 表与业务库位于同一数据库实例，保证本地事务原子性。
- 扫描、清理、重试加速任务由每个实例运行：逐行抢占保证不会重复发布，因此**无需强制选主**；数据量大时可选用 `LockManager` 减少空扫。
- 要求所有实例使用 NTP 同步时间源；数据库列统一为 `DATETIME(3)`（UTC），读写由应用显式转换，不依赖 JVM 或数据库会话的默认时区。
- `partition_key` 仅用于 Broker 路由与消费分组，组件不按其串行发布；需要顺序的接入方应自行确认 Broker 分区与消费端串行策略。

### 10.2 监控指标

建议通过 `MessageListener` + Micrometer 采集：

- Outbox 表总记录数、各状态数量。
- INIT 消息积压量、最大滞留时间。
- FAILED 消息数量、重试次数分布。
- DEAD 消息数量（关键告警指标）。
- 投递成功率、平均投递延迟、发布超时次数。
- 扫描耗时、单轮扫描条数、线程池拒绝次数。
- `markSent` 失败次数（重复投递风险指标）。
- 清理任务执行频率与删除行数。

### 10.3 告警策略

- DEAD 消息出现立即告警，告警内容带 `msgId / bizCode / topic / retryCount / lastError`。
- INIT 积压超过阈值告警。
- 投递成功率低于阈值告警。
- `markSent` 失败、线程池持续拒绝、扫描长时间未执行告警。
- Outbox 表数据量持续增长告警；DEAD 数量增长单独告警。
- 告警需要去重和限流，避免死信风暴。

### 10.4 清理与归档

- SENT 消息默认保留 7 天，分批清理。
- DEAD 消息不自动删除，但建议定期归档到 `t_outbox_dead_archive`，避免主表无限膨胀影响扫描性能。
- 归档前需确认已完成人工处理，归档操作保留审计记录。

## 11. 命名决策

- **组件与仓库名**：`reliable-outbox`。
- **Maven 模块**：`reliable-outbox-core`、`reliable-outbox-spring-boot-starter` 等；使用独立制品坐标和版本，不继承 rapidf 父工程，也不要求通过 rapidf BOM 引入。
- **Java 包名**：使用仓库所有者控制的中立反向域名，包路径以 `reliableoutbox` 表意；不得使用 `io.rapidf.*` 命名空间。实际 groupId 和根包名在仓库初始化时确定。
- **Java 类型与 bean 命名**：组件专属类型使用 `ReliableOutbox` 前缀，Spring bean 使用 `reliableOutbox` 前缀。
- **消息表名**：`t_outbox`，保留 Outbox Pattern 技术语义，不随组件品牌改名。
- **配置前缀**：统一使用 `reliable-outbox`；表名通过 `reliable-outbox.table-name` 自定义，默认 `t_outbox`。默认 Repository 只接受匹配 `[A-Za-z_][A-Za-z0-9_]{0,63}` 的非限定表名，校验通过后才可拼接到 SQL；该配置在启动时固定，不支持按请求、消息或租户动态切表。
- **顺序语义命名**：组件不提供顺序能力，因此不保留 `ordered / orderKey / orderSeq` 等对外字段；`partition_key` 仅表示 Broker 分区/消费分组维度。
- **通用领域命名**：对外入口统一为 `MessageClient`；存储接口统一为 `OutboxRepository`；事务抽象统一为 `TransactionExecutor`；扫描器统一为 `MessageScanner`。

## 12. 演进路线

1. reliable-outbox 从首个版本起在独立仓库中维护，独立构建、测试、版本化和发布。
2. 核心制品使用仓库自己的 groupId、artifactId 和版本，不继承 rapidf 父工程，不依赖 rapidf BOM，也不要求 rapidf 运行时。
3. rapidf 及其他项目均作为普通下游消费者，通过公开制品坐标按需引入；下游 BOM 可以选择管理 reliable-outbox 版本，但这不是 reliable-outbox 的依赖或发布前提。
4. 逐步完善多 MQ 适配、监控面板、死信归档与运维工具。
5. 首个版本只承诺可靠投递；后续评估 Claim Check 大消息、死信归档工具与批量回放。
6. 如确有“严格投递顺序”需求，优先评估 CDC（binlog/WAL 订阅）或数据库原生事务队列，不把本地消息表当作顺序通道。

## 13. 关键约束与待定项

已明确的约束：

- 投递语义：至少一次，消费端幂等是硬要求；重试、租约回收与崩溃恢复都会产生重复。
- 顺序性：**非目标**。组件不保证同一 `partition_key` 内的投递顺序；需要顺序的业务由接入方（生产端串行提交 + Broker 分区）与消费端（按键串行或版本号条件更新）自行保证。
- 大消息：默认 payload 上限 64KB（65,535 字节，`TEXT` 容量）、headers 上限 4KB（4,096 字节），超出抛出 `MessageTooLargeException` 且不落库；Claim Check 暂不内置。
- 软删除：`deleted=1` 仅阻止后续投递，不撤回已发出的消息；软删除对所有状态一致适用。
- 事务：`sendInTransaction` 以 MANDATORY 加入当前物理事务；`sendStandalone` 以 REQUIRES_NEW 独立提交。任一入口都只对实际参与的事务边界负责。
- 时间：数据库统一 `DATETIME(3)`（UTC，毫秒精度），代码层使用 `Instant` 并显式按 UTC 转换；不依赖 JVM/JDBC 默认时区。
- 多实例：租约抢占保证正确性；扫描选主仅用于降低开销。
- 发布契约：`publish` 正常返回即 Broker 已确认；发布超时必须小于租约。

后续待定：

- 是否需要提供管理端接口（死信列表、详情、回放、指标查询）及权限模型。
- 是否内置多租户隔离（按 `biz_code` 分表或分库）。
- 是否提供与具体 MQ 的开箱即用适配包及幂等生产者配置模板。

## 14. 文档修订记录

- 修复 `insert` 后主键未回填导致消息无法抢占的问题。
- 修复自动装配中的两处循环依赖，并将死信回放拆分为 `DeadLetterReplayer`。
- 移除超出 MySQL `DATETIME` 范围的 `LocalDateTime.MAX`；时间字段曾改为 UTC epoch millis 以消除时区与精度问题（后续调整为 `DATETIME(3)`，见文末）。
- 统一通过自动配置注册 Repository 等 bean，移除 `@Component` 与 `@Bean` 重复定义，补齐 `@ConditionalOnMissingBean`。
- `afterCommit` 改为提交到有界线程池，业务线程不阻塞，异常不再向业务方传播。
- 合并 `RetryScanScheduler` 与 `OrphanScanner` 为 `MessageScanner`，统一由 `next_retry_time` 驱动。
- 修正重试时间轴（封顶 300s）、抖动为 0 时的异常、清理任务无上限等问题。
- 补充索引 `(status, sent_at)`、发布确认契约、租约与发布超时关系、嵌套事务限制、背压和消息体上限。
- 新增 `(topic, partition_key)` 严格顺序投递：`ordered / order_key / order_seq` 字段、`t_outbox_order` 水位线表、顺序号与业务事务同事务分配、只投递顺序头、DEAD 阻塞与回放解阻、Broker/消费端顺序约束。（该能力已在后续修订中移除，见文末。）
- 消息大小上限升级为一等能力：新增 `MessageTooLargeException`、headers 上限，明确校验发生在序列化之后、落库之前，并与 Broker/数据库限制取最小值。
- `payload` 列由 `MEDIUMTEXT` 调整为 `TEXT`，默认消息上限同步收紧为 64KB（65,535 字节），使应用层上限与数据库容量一致；如需更大消息必须显式改列并调整配置。
- `headers` 列由 `JSON` 调整为 `TEXT`，默认上限由 32KB 收紧为 **4KB**：headers 定位为透传元数据，组件不做结构化解析与查询，避免大字段被误用为第二个 payload；如需在数据库中按 header 查询，应拆独立 `VARCHAR` 列。
- 时间列由 `BIGINT` epoch millis 调整为 `DATETIME(3)`（UTC，毫秒精度）：`next_retry_time`、`owner_expire_at`、`created_at`、`updated_at`、`sent_at`。代码层保留 `Instant`，读写通过 `toDbTime()` / `toInstant()` 显式按 UTC 转换，不依赖 JVM 或 JDBC 会话时区。
- 移除 `replay_count` 与 `reliable-outbox.max-replay-count`：回放是否允许只取决于当前状态是否为 DEAD；防循环与回放频率改由管理端的权限、审计、告警和频率限制控制，避免行级计数在达到上限后造成无法回放的运维死胡同。
- 新增 `deleted` 软删除标记（0 正常 / 1 删除）：扫描、租约抢占、回放与投递前检查均过滤 `deleted=0`；`uk_msg_id` 仍占用该 ID。
- **收缩能力边界**：移除 `ordered / order_key / order_seq`、`uk_order_seq`、`t_outbox_order` 表与顺序号分配器。reliable-outbox 只承诺至少一次可靠投递，不再保证发送顺序；`afterCommit` 仍作为低延迟快路径保留（异步、尽力而为），顺序由接入方（串行提交 + `partition_key` 路由）与消费端（按键串行或版本号条件更新）负责。触发原因：轮询型 outbox 在并发提交、重试与多实例场景下无法可靠表达提交顺序，强有序会引入写时行锁、队头阻塞与 DEAD 卡死整条 key，与组件的可靠性目标冲突。
- 组件统一命名为 `reliable-outbox`，采用独立仓库、独立构建与独立发布；移除对 rapidf 的项目归属、命名空间和依赖治理耦合，rapidf 仅可作为普通下游消费者接入。
- 将 `reliable-outbox.init-scan-grace` 默认值从 5 分钟缩短为 30 秒；结合默认 30 秒扫描周期，主动触发失效时的兜底发现窗口约为 30～60 秒。
- 完善 Outbox 数据库表自定义命名：通过 `reliable-outbox.table-name` 在启动时配置，默认 `t_outbox`；补充表名白名单、长度限制、DDL 同步和禁止运行时动态切表等约束。
- 合并发送入口的内部实现：保留 `sendInTransaction` 与 `sendStandalone` 两个明确入口，统一复用 `doSend`、Outbox 落库、`afterCommit` 触发和扫描重试流程；事务执行分别采用 MANDATORY 与 REQUIRES_NEW。