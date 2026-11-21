# reliable-outbox 使用示例（Kafka）

本示例按「下单」场景演示 `reliable-outbox` 的完整使用方式：

1. 订单与 Outbox 记录在同一个本地事务内写入；
2. 事务提交后立即投递到 Kafka；
3. 投递失败按退避策略重试，超过上限转为死信；
4. 死信可手动重放；
5. 应用重启或崩溃后，由扫描任务兜底投递未完成的消息。

示例使用 H2 内存库和本地 Docker Kafka，启动后即可通过 HTTP 接口操作和观察。

## 1. 前置条件

- JDK 17
- Maven 3.8+
- Docker（用于启动 Kafka）

## 2. 启动 Kafka

```bash
cd reliable-outbox-sample/reliable-outbox-sample-kafka
docker compose up -d
```

## 3. 构建并启动示例应用

在仓库根目录执行：

```bash
# 安装示例依赖的模块
mvn -q -pl reliable-outbox-sample/reliable-outbox-sample-kafka -am install -Dmaven.test.skip=true

# 启动应用
mvn -pl reliable-outbox-sample/reliable-outbox-sample-kafka spring-boot:run
```

应用启动在 `http://localhost:8080`，启动时执行 `schema.sql` 创建业务表 `t_order` 与 Outbox 表 `t_outbox`。

## 4. 下单并观察投递

提交订单：

```bash
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"productId":"sku-1","quantity":2,"amount":19.90}'
```

返回示例（记下 `id`）：

```json
{"id":"7e434287afd7496db9a07c4efa5cf244","productId":"sku-1","quantity":2,"amount":19.90,"createdAt":"2026-11-20T23:02:30.341"}
```

依次查看订单、Outbox 状态和消费端收到的消息：

```bash
curl -s http://localhost:8080/orders/<orderId>
curl -s http://localhost:8080/outbox
curl -s http://localhost:8080/consumed
```

预期结果：

- `/outbox` 中该消息 `status` 为 `SENT`，表示已成功投递；
- `/consumed` 中能看到 `order-events` 的消息，说明端到端投递成功。

| 接口 | 说明 |
| --- | --- |
| `POST /orders` | 下单，事务内写订单与 Outbox |
| `GET /orders/{id}` | 查询订单 |
| `GET /outbox` | 查看最近 50 条 Outbox 记录 |
| `GET /consumed` | 查看示例消费端收到的消息 |
| `POST /outbox/{msgId}/replay` | 重放死信 |

## 5. 演示失败重试与死信重放

1. 停止 Kafka：

```bash
cd reliable-outbox-sample/reliable-outbox-sample-kafka
docker compose stop
```

2. 再次调用 `POST /orders` 下单。

3. 反复查看 `/outbox`：消息先处于 `PENDING`，`retryCount` 随重试增长，`lastError` 记录失败原因；超过 `max-count`（示例配置为 3）后变为 `DEAD`。

4. 恢复 Kafka：

```bash
docker compose start
```

5. 重放死信（`msgId` 取自 `/outbox`）：

```bash
curl -s -X POST http://localhost:8080/outbox/<msgId>/replay
```

6. 再次查看 `/outbox`，该消息变为 `SENT`；`/consumed` 中可以看到它。

## 6. 代码导读

| 关注点 | 位置 |
| --- | --- |
| 事务内写业务数据与 Outbox | `reliable-outbox-sample-common/.../service/OrderService.java` |
| 事务提交后投递、扫描兜底 | `OrderService` 调用 `messageClient.sendInTransaction`，`OutboxJobs` 定时调用 `messageScanner.scan()` |
| Outbox 查看与死信重放 | `reliable-outbox-sample-common/.../web/OutboxController.java` |
| Kafka 消费端 | `reliable-outbox-sample-kafka/.../OrderEventListener.java` |

订阅消息时使用 Kafka 消息头 `outbox-msg-id` 作为消费端幂等键：投递语义为至少一次，消费端需要按该键去重。

示例为便于观察，调小了 `initial-grace`、`lease-duration` 和重试间隔，配置见 `reliable-outbox-sample-kafka/src/main/resources/application.yml`。

## 7. 运行测试

```bash
mvn -pl reliable-outbox-sample/reliable-outbox-sample-kafka test
```

测试使用 stub `MessagePublisher` 替换 Kafka 适配器，无需启动 Broker，验证下单后订单与 Outbox 记录落库并最终标记 `SENT`。
