# reliable-outbox 使用示例（RocketMQ）

本示例按「下单」场景演示 `reliable-outbox` 对 RocketMQ 的集成：

1. 订单与 Outbox 记录在同一个本地事务内写入；
2. 事务提交后立即投递到 RocketMQ，并等待 Broker 确认；
3. 投递失败按退避策略重试，超过上限转为死信；
4. 死信可手动重放；
5. 应用重启或崩溃后，由扫描任务兜底投递未完成的消息。

示例使用 H2 内存库和本地 Docker RocketMQ（5.x），启动后即可通过 HTTP 接口操作和观察。

## 1. 前置条件

- JDK 17
- Maven 3.8+
- Docker（用于启动 RocketMQ）

## 2. 启动 RocketMQ

```bash
cd reliable-outbox-sample/reliable-outbox-sample-rocketmq
docker compose up -d
```

`docker compose` 会启动 NameServer（`localhost:9876`）与 Broker（`localhost:10911`）。
`broker.conf` 中固定了 `brokerIP1=127.0.0.1` 并开启 `autoCreateTopicEnable`，仅供本机示例使用；
若客户端在其他机器上，请把 `brokerIP1` 改成宿主可访问的 IP。

## 3. 构建并启动示例应用

在仓库根目录执行：

```bash
# 安装示例依赖的模块
mvn -q -pl reliable-outbox-sample/reliable-outbox-sample-rocketmq -am install -Dmaven.test.skip=true

# 启动应用
mvn -pl reliable-outbox-sample/reliable-outbox-sample-rocketmq spring-boot:run
```

应用启动在 `http://localhost:8081`，启动时执行 `schema.sql` 创建业务表 `t_order` 与 Outbox 表 `t_outbox`。

## 4. 下单并观察投递

提交订单：

```bash
curl -s -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"productId":"sku-1","quantity":2,"amount":19.90}'
```

返回示例（记下 `id`）：

```json
{"id":"7e434287afd7496db9a07c4efa5cf244","productId":"sku-1","quantity":2,"amount":19.90,"createdAt":"2026-11-20T23:02:30.341"}
```

依次查看订单、Outbox 状态和消费端收到的消息：

```bash
curl -s http://localhost:8081/orders/<orderId>
curl -s http://localhost:8081/outbox
curl -s http://localhost:8081/consumed
```

预期结果：

- `/outbox` 中该消息 `status` 为 `SENT`，表示 Broker 已确认；
- `/consumed` 中能看到 `order-events` 的消息，说明端到端投递成功。

| 接口 | 说明 |
| --- | --- |
| `POST /orders` | 下单，事务内写订单与 Outbox |
| `GET /orders/{id}` | 查询订单 |
| `GET /outbox` | 查看最近 50 条 Outbox 记录 |
| `GET /consumed` | 查看示例消费端收到的消息 |
| `POST /outbox/{msgId}/replay` | 重放死信 |

## 5. 演示失败重试与死信重放

1. 停止 RocketMQ：

```bash
cd reliable-outbox-sample/reliable-outbox-sample-rocketmq
docker compose stop
```

2. 再次调用 `POST /orders` 下单。

3. 反复查看 `/outbox`：消息先处于 `PENDING`，`retryCount` 随重试增长，`lastError` 记录失败原因；超过 `max-count`（示例配置为 3）后变为 `DEAD`。

4. 恢复 RocketMQ：

```bash
docker compose start
```

5. 重放死信（`msgId` 取自 `/outbox`）：

```bash
curl -s -X POST http://localhost:8081/outbox/<msgId>/replay
```

6. 再次查看 `/outbox`，该消息变为 `SENT`；`/consumed` 中可以看到它。

## 6. 代码导读

| 关注点 | 位置 |
| --- | --- |
| 事务内写业务数据与 Outbox | `reliable-outbox-sample-common/.../service/OrderService.java` |
| 事务提交后投递、扫描兜底 | `OrderService` 调用 `messageClient.sendInTransaction`，`OutboxJobs` 定时调用 `messageScanner.scan()` |
| Outbox 查看与死信重放 | `reliable-outbox-sample-common/.../web/OutboxController.java` |
| RocketMQ 消费端 | `reliable-outbox-sample-rocketmq/.../RocketMQOrderListener.java` |

Outbox 的 `msgId` 同时写入 RocketMQ 的 `keys`（便于控制台/`mqadmin queryMsgByKey` 检索）和用户属性 `outbox-msg-id`。
消费端以用户属性 `outbox-msg-id` 作为幂等键：投递语义为至少一次，需要按该键去重。
RocketMQ 自身的消息 ID 由 Broker 在写入 CommitLog 时生成，无法用于业务幂等。

`partition_key` 对 RocketMQ 不做处理：Outbox 的重试与并发投递本身不保证顺序，因此不存在分区/队列选择语义。

示例为便于观察，调小了 `initial-grace`、`lease-duration` 和重试间隔，配置见 `reliable-outbox-sample-rocketmq/src/main/resources/application.yml`。

## 7. 运行测试

```bash
mvn -pl reliable-outbox-sample/reliable-outbox-sample-rocketmq test
```

测试使用 stub `MessagePublisher` 替换 RocketMQ 适配器并关闭消费端，无需启动 Broker，验证下单后订单与 Outbox 记录落库并最终标记 `SENT`。
