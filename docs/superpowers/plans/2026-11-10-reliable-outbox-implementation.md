# reliable-outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `reliable-outbox` library exactly as specified in `reliable-outbox.md`: a Transactional Outbox component with atomic local persistence, asynchronous at-least-once delivery, pluggable MQ/retry integration, lease-based concurrency safety, retry, cleanup and dead-letter replay.

**Architecture:** Maven multi-module build. `reliable-outbox-core` contains the domain model, SPI interfaces, JDBC repository, transaction executor, dispatcher, scanner, cleaner and dead-letter components. `reliable-outbox-spring-boot-starter` contains the Spring Boot auto-configuration and depends on core. Delivery is driven by `afterCommit` (fast path) plus a database scanner (reliable fallback); correctness relies on conditional-update leases (`owner_token` + `owner_expire_at`), never on scheduler reliability.

**Tech Stack:** Java 17, Spring Boot 4.1.1 (BOM only), Spring JDBC / TX, Lombok, JUnit 6 + Mockito + AssertJ (via `spring-boot-starter-test`), H2 2.x in MySQL mode for repository tests, Maven 3.8+.

## Global Constraints

- GroupId `com.lazycece`; root package `com.lazycece.outbox`; artifacts `reliable-outbox-parent`, `reliable-outbox-core`, `reliable-outbox-spring-boot-starter`; version `0.1.0-SNAPSHOT`.
- Java 17 (`maven.compiler.release=17`), Spring Boot BOM `4.1.1`, Maven compiler `3.13.0`, Surefire `3.5.6`, Lombok `1.18.46`.
- Delivery semantics: at least once. Ordering is explicitly NOT guaranteed. No `SENDING` status.
- All DB timestamps are `DATETIME(3)` in UTC; Java uses `Instant`; conversion is always explicit via `LocalDateTime.ofInstant(instant.truncatedTo(MILLIS), ZoneOffset.UTC)` and `localDateTime.toInstant(ZoneOffset.UTC)`. Never rely on JVM/JDBC default timezone.
- Table name comes from `reliable-outbox.table-name`, validated against `[A-Za-z_][A-Za-z0-9_]{0,63}` before concatenation. Never hard-code `t_outbox` in SQL.
- `sendInTransaction` uses `PROPAGATION_MANDATORY`; `sendStandalone` uses `PROPAGATION_REQUIRES_NEW`.
- Defaults: `dispatchConcurrency=8`, `dispatchQueueCapacity=1000`, `ownerLease=60s`, `publishTimeout=20s`, `initScanGrace=30s`, `scanInterval=30s`, `scanBatchSize=100`, `maxScanBatchesPerRun=10`, `maxRetryCount=16`, `initialRetryInterval=5s`, `maxRetryInterval=300s`, `retryJitter=1000ms`, `sentRetention=7d`, `cleanInterval=1h`, `cleanBatchSize=1000`, `cleanMaxBatchesPerRun=10`, `maxPayloadBytes=65535`, `maxHeadersBytes=4096`.
- Startup validation: `ownerLease > publishTimeout`; `1 <= maxPayloadBytes <= 65535`; `1 <= maxHeadersBytes <= 65535`.
- `MessagePublisher` is mandatory and has no built-in implementation; a missing bean must fail startup with a clear error.
- The component never creates tables. Ship the canonical MySQL DDL as a resource only.
- Optional SPIs `MessageConverter` and `LockManager` are NOT implemented in this version; they are documented extension points only. No unused abstractions.
- Tests must run with `mvn test` on this machine (no Docker). Repository integration tests use H2 with `MODE=MySQL` and a dedicated H2-compatible schema.
- Every task ends with a green test run and a git commit.
- **Execution order:** `1, 2, 3, 4, 5, 7, 8, 9, 6, 10, 11, 12, 13, 14`. Task 6 depends on the real `MessageDispatcher` from Task 9; Task 8 temporarily creates a stub dispatcher so the retry scheduler compiles, and Task 9 replaces that stub with the full implementation. Do not start Task 6 before Task 9.

---

### Task 1: Maven multi-module skeleton

**Files:**
- Create: `pom.xml`
- Create: `reliable-outbox-core/pom.xml`
- Create: `reliable-outbox-spring-boot-starter/pom.xml`
- Modify: `.gitignore` (append `target/`, `*.iml`, `.classpath`, `.project`, `.settings/`)

**Interfaces:**
- Consumes: nothing.
- Produces: parent POM with dependency/plugin management; both modules resolve and build empty jars.

- [ ] **Step 1: Create the parent POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.lazycece</groupId>
    <artifactId>reliable-outbox-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>reliable-outbox-parent</name>
    <description>Reliable transactional outbox component</description>

    <modules>
        <module>reliable-outbox-core</module>
        <module>reliable-outbox-spring-boot-starter</module>
    </modules>

    <properties>
        <java.version>17</java.version>
        <maven.compiler.release>17</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
        <spring-boot.version>4.1.1</spring-boot.version>
        <lombok.version>1.18.46</lombok.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>com.lazycece</groupId>
                <artifactId>reliable-outbox-core</artifactId>
                <version>${project.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.13.0</version>
                    <configuration>
                        <release>${maven.compiler.release}</release>
                        <annotationProcessorPaths>
                            <path>
                                <groupId>org.projectlombok</groupId>
                                <artifactId>lombok</artifactId>
                                <version>${lombok.version}</version>
                            </path>
                        </annotationProcessorPaths>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>3.5.6</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [ ] **Step 2: Create the core POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.lazycece</groupId>
        <artifactId>reliable-outbox-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>reliable-outbox-core</artifactId>
    <name>reliable-outbox-core</name>

    <dependencies>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-tx</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot</artifactId>
        </dependency>
        <dependency>
            <groupId>jakarta.annotation</groupId>
            <artifactId>jakarta.annotation-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
            <optional>true</optional>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: Create the starter POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.lazycece</groupId>
        <artifactId>reliable-outbox-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>reliable-outbox-spring-boot-starter</artifactId>
    <name>reliable-outbox-spring-boot-starter</name>

    <dependencies>
        <dependency>
            <groupId>com.lazycece</groupId>
            <artifactId>reliable-outbox-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-autoconfigure</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
            <optional>true</optional>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: Update `.gitignore`**

Append exactly:

```gitignore
target/
*.iml
.classpath
.project
.settings/
```

- [ ] **Step 5: Verify the empty build**

Run: `mvn -q -DskipTests package`
Expected: BUILD SUCCESS, both module jars produced.

- [ ] **Step 6: Commit**

```bash
git add pom.xml reliable-outbox-core/pom.xml reliable-outbox-spring-boot-starter/pom.xml .gitignore
git commit -m "build: add reliable-outbox multi-module skeleton"
```

---

### Task 2: Domain model and configuration properties

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/model/OutboxStatus.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/model/OutboxMessage.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/config/ReliableOutboxProperties.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/model/OutboxStatusTest.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/config/ReliableOutboxPropertiesTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `OutboxStatus.isDispatchable()`, `OutboxMessage` (Lombok getters/setters/equals/hashCode/toString), `ReliableOutboxProperties` with all defaults and `validate()`.

- [ ] **Step 1: Write the failing tests**

`OutboxStatusTest.java`:

```java
package com.lazycece.outbox.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OutboxStatusTest {

    @Test
    void onlyInitAndFailedAreDispatchable() {
        assertThat(OutboxStatus.INIT.isDispatchable()).isTrue();
        assertThat(OutboxStatus.FAILED.isDispatchable()).isTrue();
        assertThat(OutboxStatus.SENT.isDispatchable()).isFalse();
        assertThat(OutboxStatus.DEAD.isDispatchable()).isFalse();
    }
}
```

`ReliableOutboxPropertiesTest.java`:

```java
package com.lazycece.outbox.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReliableOutboxPropertiesTest {

    @Test
    void defaultsMatchTheDesign() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        assertThat(p.isEnabled()).isTrue();
        assertThat(p.getTableName()).isEqualTo("t_outbox");
        assertThat(p.getOwnerLease()).isEqualTo(Duration.ofSeconds(60));
        assertThat(p.getPublishTimeout()).isEqualTo(Duration.ofSeconds(20));
        assertThat(p.getInitScanGrace()).isEqualTo(Duration.ofSeconds(30));
        assertThat(p.getScanInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(p.getMaxRetryCount()).isEqualTo(16);
        assertThat(p.getMaxPayloadBytes()).isEqualTo(65535);
        assertThat(p.getMaxHeadersBytes()).isEqualTo(4096);
        p.validate();
    }

    @Test
    void leaseMustBeGreaterThanPublishTimeout() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        p.setOwnerLease(Duration.ofSeconds(20));
        p.setPublishTimeout(Duration.ofSeconds(20));
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("owner-lease");
    }

    @Test
    void payloadLimitMustFitTextColumn() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        p.setMaxPayloadBytes(65536);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max-payload-bytes");

        p.setMaxPayloadBytes(0);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max-payload-bytes");
    }

    @Test
    void headersLimitMustFitTextColumn() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        p.setMaxHeadersBytes(65536);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max-headers-bytes");

        p.setMaxHeadersBytes(0);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max-headers-bytes");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=OutboxStatusTest,ReliableOutboxPropertiesTest`
Expected: compilation failure — `OutboxStatus` / `ReliableOutboxProperties` do not exist.

- [ ] **Step 3: Implement the model and properties**

`OutboxStatus.java`:

```java
package com.lazycece.outbox.model;

public enum OutboxStatus {
    INIT, FAILED, SENT, DEAD;

    public boolean isDispatchable() {
        return this == INIT || this == FAILED;
    }
}
```

`OutboxMessage.java`:

```java
package com.lazycece.outbox.model;

import java.time.Instant;
import lombok.Data;
import lombok.ToString;

@Data
public class OutboxMessage {
    private Long id;
    private String msgId;
    private String bizCode;
    private String topic;
    /** Broker partition/queue routing only. No ordering guarantee. */
    private String partitionKey;
    @ToString.Exclude
    private String payload;
    @ToString.Exclude
    private String headers;
    private OutboxStatus status;
    /** Soft-delete marker; deleted messages are never dispatched. */
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

`ReliableOutboxProperties.java`:

```java
package com.lazycece.outbox.config;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

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

    private int maxPayloadBytes = 64 * 1024 - 1;
    private int maxHeadersBytes = 4 * 1024;

    @PostConstruct
    public void validate() {
        if (ownerLease.compareTo(publishTimeout) <= 0) {
            throw new IllegalStateException(
                    "reliable-outbox.owner-lease 必须大于 reliable-outbox.publish-timeout");
        }
        if (maxPayloadBytes <= 0 || maxPayloadBytes > 65535) {
            throw new IllegalStateException(
                    "reliable-outbox.max-payload-bytes 必须在 1..65535 之间（TEXT 上限）；"
                            + "如需更大消息，请同时修改 payload 列类型并移除该校验");
        }
        if (maxHeadersBytes <= 0 || maxHeadersBytes > 65535) {
            throw new IllegalStateException(
                    "reliable-outbox.max-headers-bytes 必须在 1..65535 之间（TEXT 上限）");
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=OutboxStatusTest,ReliableOutboxPropertiesTest`
Expected: BUILD SUCCESS, 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/model reliable-outbox-core/src/main/java/com/lazycece/outbox/config reliable-outbox-core/src/test/java/com/lazycece/outbox/model reliable-outbox-core/src/test/java/com/lazycece/outbox/config
git commit -m "feat: add outbox model and configuration properties"
```

---

### Task 3: Publisher and listener SPI

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/spi/PublishException.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/spi/NonRetryablePublishException.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/spi/MessagePublisher.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/spi/MessageListener.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/spi/LoggingMessageListener.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/spi/PublishExceptionTest.java`

**Interfaces:**
- Consumes: `OutboxMessage`.
- Produces: `MessagePublisher.publish(OutboxMessage) throws PublishException`; `MessageListener` defaults `onSent`, `onFailed`, `onDead`, `onLeaseLost`; `NonRetryablePublishException extends PublishException`; `LoggingMessageListener`.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.spi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PublishExceptionTest {

    @Test
    void nonRetryableIsAPublishException() {
        PublishException retryable = new PublishException("retry", null);
        PublishException nonRetryable = new NonRetryablePublishException("bad topic", null);
        assertThat(nonRetryable).isInstanceOf(PublishException.class);
        assertThat(retryable).isNotInstanceOf(NonRetryablePublishException.class);
        assertThat(nonRetryable.getMessage()).isEqualTo("bad topic");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=PublishExceptionTest`
Expected: compilation failure — `PublishException` does not exist.

- [ ] **Step 3: Implement the SPI**

`PublishException.java`:

```java
package com.lazycece.outbox.spi;

public class PublishException extends RuntimeException {
    public PublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

`NonRetryablePublishException.java`:

```java
package com.lazycece.outbox.spi;

public class NonRetryablePublishException extends PublishException {
    public NonRetryablePublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

`MessagePublisher.java`:

```java
package com.lazycece.outbox.spi;

import com.lazycece.outbox.model.OutboxMessage;

public interface MessagePublisher {
    /**
     * Normal return means the broker acknowledged the message.
     * Retryable failures throw {@link PublishException};
     * non-retryable failures throw {@link NonRetryablePublishException}.
     */
    void publish(OutboxMessage message) throws PublishException;
}
```

`MessageListener.java`:

```java
package com.lazycece.outbox.spi;

import com.lazycece.outbox.model.OutboxMessage;

public interface MessageListener {
    default void onSent(OutboxMessage message) {
    }

    default void onFailed(OutboxMessage message, int retryCount, Throwable error) {
    }

    default void onDead(OutboxMessage message) {
    }

    default void onLeaseLost(OutboxMessage message, String reason) {
    }
}
```

`LoggingMessageListener.java`:

```java
package com.lazycece.outbox.spi;

import com.lazycece.outbox.model.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=PublishExceptionTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/spi reliable-outbox-core/src/test/java/com/lazycece/outbox/spi
git commit -m "feat: add publisher and listener SPI"
```

---

### Task 4: Outbox repository and JDBC implementation

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/repository/OutboxRepository.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/repository/JdbcOutboxRepository.java`
- Create: `reliable-outbox-core/src/main/resources/ddl/mysql/t_outbox.sql`
- Create: `reliable-outbox-core/src/test/resources/schema-h2.sql`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/repository/JdbcOutboxRepositoryTest.java`

**Interfaces:**
- Consumes: `OutboxMessage`, `OutboxStatus`.
- Produces: `OutboxRepository` with `insertIfAbsent`, `findById`, `findByMsgId`, `tryAcquire`, `tryAcquireDue`, `releaseLease`, `markSent`, `markFailed`, `resetForReplay`, `scanDispatchableIds`, `scanSentBefore`, `deleteSent`. `JdbcOutboxRepository(JdbcTemplate, String tableName)`.

- [ ] **Step 1: Write the failing repository test**

`schema-h2.sql` (create first so tests can run once the class exists):

```sql
CREATE TABLE IF NOT EXISTS t_outbox (
    id BIGINT NOT NULL AUTO_INCREMENT,
    msg_id VARCHAR(64) NOT NULL,
    biz_code VARCHAR(64) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) DEFAULT NULL,
    payload CLOB NOT NULL,
    headers CLOB DEFAULT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'INIT',
    deleted TINYINT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_time TIMESTAMP(3) DEFAULT NULL,
    owner_token VARCHAR(64) DEFAULT NULL,
    owner_expire_at TIMESTAMP(3) DEFAULT NULL,
    created_at TIMESTAMP(3) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL,
    sent_at TIMESTAMP(3) DEFAULT NULL,
    last_error VARCHAR(1000) DEFAULT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_msg_id UNIQUE (msg_id)
);
CREATE INDEX IF NOT EXISTS idx_status_next_retry ON t_outbox (status, next_retry_time);
CREATE INDEX IF NOT EXISTS idx_status_sent ON t_outbox (status, sent_at);
```

`JdbcOutboxRepositoryTest.java`:

```java
package com.lazycece.outbox.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

class JdbcOutboxRepositoryTest {

    private JdbcTemplate jdbcTemplate;
    private JdbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        DatabasePopulatorUtils.execute(
                new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")), dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        repository = new JdbcOutboxRepository(jdbcTemplate, "t_outbox");
    }

    private OutboxMessage message(String msgId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPartitionKey("order-1");
        message.setPayload("{\"id\":1}");
        message.setHeaders("{}");
        message.setStatus(OutboxStatus.INIT);
        message.setRetryCount(0);
        message.setNextRetryTime(now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return message;
    }

    @Test
    void insertIfAbsentFillsIdAndRejectsDuplicateMsgId() {
        OutboxMessage message = message("m1");
        assertThat(repository.insertIfAbsent(message)).isTrue();
        assertThat(message.getId()).isNotNull();

        OutboxMessage duplicate = message("m1");
        assertThat(repository.insertIfAbsent(duplicate)).isFalse();
        assertThat(duplicate.getId()).isNull();
    }

    @Test
    void findByIdAndByMsgIdRoundTripAllFields() {
        OutboxMessage message = message("m1");
        repository.insertIfAbsent(message);

        OutboxMessage byId = repository.findById(message.getId());
        assertThat(byId).isEqualTo(message);
        assertThat(repository.findByMsgId("m1")).isEqualTo(message);
        assertThat(repository.findById(-1L)).isNull();
        assertThat(repository.findByMsgId("missing")).isNull();
    }

    @Test
    void tryAcquireRespectsStatusDeletedAndLease() {
        OutboxMessage message = message("m1");
        repository.insertIfAbsent(message);
        Instant now = Instant.now();

        assertThat(repository.tryAcquire(message.getId(), "t1", now.plusSeconds(60), now)).isTrue();
        assertThat(repository.tryAcquire(message.getId(), "t2", now.plusSeconds(60), now)).isFalse();
        assertThat(repository.tryAcquire(message.getId(), "t2",
                now.plusSeconds(60), now.plusSeconds(61))).isTrue();
        assertThat(repository.findById(message.getId()).getOwnerToken()).isEqualTo("t2");

        jdbcTemplate.update("UPDATE t_outbox SET deleted = 1 WHERE id = ?", message.getId());
        assertThat(repository.tryAcquire(message.getId(), "t3",
                now.plusSeconds(60), now.plusSeconds(120))).isFalse();
    }

    @Test
    void tryAcquireDueOnlyAfterNextRetryTime() {
        OutboxMessage message = message("m1");
        repository.insertIfAbsent(message);
        Instant due = message.getNextRetryTime();

        assertThat(repository.tryAcquireDue(message.getId(), "t1",
                due.plusSeconds(60), due.minusSeconds(1))).isFalse();
        assertThat(repository.tryAcquireDue(message.getId(), "t1",
                due.plusSeconds(60), due)).isTrue();
    }

    @Test
    void releaseLeaseRequiresMatchingToken() {
        OutboxMessage message = message("m1");
        repository.insertIfAbsent(message);
        Instant now = Instant.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);

        assertThat(repository.releaseLease(message.getId(), "other", now)).isFalse();
        assertThat(repository.releaseLease(message.getId(), "owner", now)).isTrue();
        assertThat(repository.findById(message.getId()).getOwnerToken()).isNull();
    }

    @Test
    void markSentRequiresLeaseOwner() {
        OutboxMessage message = message("m1");
        repository.insertIfAbsent(message);
        Instant now = Instant.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);

        assertThat(repository.markSent(message.getId(), "other", now)).isFalse();
        assertThat(repository.markSent(message.getId(), "owner", now)).isTrue();

        OutboxMessage sent = repository.findById(message.getId());
        assertThat(sent.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.getOwnerToken()).isNull();
        assertThat(sent.getNextRetryTime()).isNull();
        assertThat(sent.getLastError()).isNull();
        assertThat(sent.getSentAt()).isEqualTo(now.truncatedTo(ChronoUnit.MILLIS));
    }

    @Test
    void markFailedWritesRetryOrDeadState() {
        OutboxMessage first = message("m1");
        repository.insertIfAbsent(first);
        Instant now = Instant.now();
        repository.tryAcquire(first.getId(), "owner", now.plusSeconds(60), now);
        Instant next = now.plusSeconds(30);

        assertThat(repository.markFailed(first.getId(), "owner", "boom", next, 1, false, now)).isTrue();
        OutboxMessage failed = repository.findById(first.getId());
        assertThat(failed.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(failed.getRetryCount()).isEqualTo(1);
        assertThat(failed.getNextRetryTime()).isEqualTo(next.truncatedTo(ChronoUnit.MILLIS));
        assertThat(failed.getLastError()).isEqualTo("boom");
        assertThat(failed.getOwnerToken()).isNull();

        repository.tryAcquire(first.getId(), "owner2", now.plusSeconds(60), now);
        assertThat(repository.markFailed(first.getId(), "owner2", "fatal", null, 16, true, now)).isTrue();
        OutboxMessage dead = repository.findById(first.getId());
        assertThat(dead.getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(dead.getRetryCount()).isEqualTo(16);
        assertThat(dead.getNextRetryTime()).isNull();
        assertThat(dead.getLastError()).isEqualTo("fatal");
    }

    @Test
    void resetForReplayOnlyResetsNonDeletedDeadMessages() {
        OutboxMessage message = message("m1");
        repository.insertIfAbsent(message);
        jdbcTemplate.update("UPDATE t_outbox SET status = 'DEAD', retry_count = 16 WHERE id = ?",
                message.getId());

        Instant now = Instant.now();
        assertThat(repository.resetForReplay("m1", now, now)).isTrue();
        OutboxMessage reset = repository.findById(message.getId());
        assertThat(reset.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(reset.getRetryCount()).isZero();
        assertThat(reset.getNextRetryTime()).isEqualTo(now.truncatedTo(ChronoUnit.MILLIS));
        assertThat(repository.resetForReplay("m1", now, now)).isFalse();

        jdbcTemplate.update("UPDATE t_outbox SET status = 'DEAD', deleted = 1 WHERE id = ?",
                message.getId());
        assertThat(repository.resetForReplay("m1", now, now)).isFalse();
        assertThat(repository.resetForReplay("missing", now, now)).isFalse();
    }

    @Test
    void scanDispatchableIdsFiltersDeletedFutureAndLeasedRows() {
        Instant now = Instant.now();
        OutboxMessage due1 = message("due1");
        due1.setNextRetryTime(now.minusSeconds(10));
        OutboxMessage due2 = message("due2");
        due2.setNextRetryTime(now.minusSeconds(5));
        OutboxMessage future = message("future");
        future.setNextRetryTime(now.plusSeconds(60));
        repository.insertIfAbsent(due1);
        repository.insertIfAbsent(due2);
        repository.insertIfAbsent(future);

        List<Long> ids = repository.scanDispatchableIds(10, now);
        assertThat(ids).containsExactly(due1.getId(), due2.getId());

        repository.tryAcquire(due1.getId(), "owner", now.plusSeconds(60), now);
        assertThat(repository.scanDispatchableIds(10, now)).containsExactly(due2.getId());

        jdbcTemplate.update("UPDATE t_outbox SET deleted = 1 WHERE id = ?", due2.getId());
        assertThat(repository.scanDispatchableIds(10, now)).isEmpty();
    }

    @Test
    void scanAndDeleteOnlyTouchSentMessagesBeforeRetention() {
        Instant now = Instant.now();
        OutboxMessage old1 = message("old1");
        OutboxMessage old2 = message("old2");
        OutboxMessage recent = message("recent");
        OutboxMessage dead = message("dead");
        repository.insertIfAbsent(old1);
        repository.insertIfAbsent(old2);
        repository.insertIfAbsent(recent);
        repository.insertIfAbsent(dead);

        Instant oldSentAt = now.minusSeconds(8 * 24 * 3600L);
        jdbcTemplate.update("UPDATE t_outbox SET status = 'SENT', sent_at = ? WHERE id = ?",
                oldSentAt, old1.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 'SENT', sent_at = ? WHERE id = ?",
                oldSentAt.plusMillis(1), old2.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 'SENT', sent_at = ? WHERE id = ?",
                now, recent.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 'DEAD', sent_at = ? WHERE id = ?",
                oldSentAt, dead.getId());

        List<Long> candidates = repository.scanSentBefore(now.minusSeconds(24 * 3600L), 10);
        assertThat(candidates).containsExactly(old1.getId(), old2.getId());
        assertThat(repository.deleteSent(candidates)).isEqualTo(2);
        assertThat(repository.findById(old1.getId())).isNull();
        assertThat(repository.findById(recent.getId()).getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(repository.findById(dead.getId()).getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(repository.deleteSent(List.of())).isZero();
    }

    @Test
    void tableNameIsValidatedBeforeUse() {
        assertThatThrownBy(() -> new JdbcOutboxRepository(jdbcTemplate, "t_outbox; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JdbcOutboxRepository(jdbcTemplate, "1abc"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JdbcOutboxRepository(jdbcTemplate, (String) null))
                .isInstanceOf(IllegalArgumentException.class);
        new JdbcOutboxRepository(jdbcTemplate, "biz_event_outbox");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=JdbcOutboxRepositoryTest`
Expected: compilation failure — `OutboxRepository` / `JdbcOutboxRepository` do not exist.

- [ ] **Step 3: Implement the repository interface**

`OutboxRepository.java`:

```java
package com.lazycece.outbox.repository;

import com.lazycece.outbox.model.OutboxMessage;
import java.time.Instant;
import java.util.List;

public interface OutboxRepository {
    /** Inserts and fills the generated id. Returns false when msg_id already exists. */
    boolean insertIfAbsent(OutboxMessage message);

    OutboxMessage findById(Long id);

    OutboxMessage findByMsgId(String msgId);

    /** Ignores next_retry_time; used for after-commit triggering and manual replay. */
    boolean tryAcquire(Long id, String ownerToken, Instant ownerExpireAt, Instant now);

    /** Also requires next_retry_time <= dueAt; used by scanner and retry accelerator. */
    boolean tryAcquireDue(Long id, String ownerToken, Instant ownerExpireAt, Instant dueAt);

    boolean releaseLease(Long id, String ownerToken, Instant now);

    boolean markSent(Long id, String ownerToken, Instant sentAt);

    /** dead=true means nextRetryTime must be null. */
    boolean markFailed(Long id, String ownerToken, String error,
                       Instant nextRetryTime, int retryCount,
                       boolean dead, Instant now);

    boolean resetForReplay(String msgId, Instant nextRetryTime, Instant now);

    /** Returns ids only; ordered by next_retry_time, id ascending. */
    List<Long> scanDispatchableIds(int limit, Instant now);

    List<Long> scanSentBefore(Instant before, int limit);

    int deleteSent(List<Long> ids);
}
```

- [ ] **Step 4: Implement `JdbcOutboxRepository`**

`JdbcOutboxRepository.java`:

```java
package com.lazycece.outbox.repository;

import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

public class JdbcOutboxRepository implements OutboxRepository {
    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

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
            // MySQL/InnoDB only: a failed single statement does not abort the transaction.
            return false;
        }
    }

    private long insert(OutboxMessage message) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + tableName + " (msg_id, biz_code, topic, partition_key, "
                            + "payload, headers, status, retry_count, next_retry_time, created_at, updated_at) "
                            + "VALUES (?, ?, ?, ?, ?, ?, 'INIT', 0, ?, ?, ?)",
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
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("插入 Outbox 消息后未获取到主键");
        }
        return key.longValue();
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
                "UPDATE " + tableName + " SET owner_token = ?, owner_expire_at = ?, updated_at = ? "
                        + "WHERE id = ? AND status IN ('INIT', 'FAILED') AND deleted = 0 "
                        + "AND (owner_token IS NULL OR owner_expire_at < ?)",
                ownerToken, toDbTime(ownerExpireAt), toDbTime(now), id, toDbTime(now));
        return rows == 1;
    }

    @Override
    public boolean tryAcquireDue(Long id, String ownerToken, Instant ownerExpireAt, Instant dueAt) {
        int rows = jdbcTemplate.update(
                "UPDATE " + tableName + " SET owner_token = ?, owner_expire_at = ?, updated_at = ? "
                        + "WHERE id = ? AND status IN ('INIT', 'FAILED') AND deleted = 0 "
                        + "AND next_retry_time IS NOT NULL AND next_retry_time <= ? "
                        + "AND (owner_token IS NULL OR owner_expire_at < ?)",
                ownerToken, toDbTime(ownerExpireAt), toDbTime(dueAt), id, toDbTime(dueAt),
                toDbTime(dueAt));
        return rows == 1;
    }

    @Override
    public boolean releaseLease(Long id, String ownerToken, Instant now) {
        int rows = jdbcTemplate.update(
                "UPDATE " + tableName + " SET owner_token = NULL, owner_expire_at = NULL, updated_at = ? "
                        + "WHERE id = ? AND owner_token = ?",
                toDbTime(now), id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean markSent(Long id, String ownerToken, Instant sentAt) {
        int rows = jdbcTemplate.update(
                "UPDATE " + tableName + " SET status = 'SENT', owner_token = NULL, owner_expire_at = NULL, "
                        + "sent_at = ?, next_retry_time = NULL, last_error = NULL, updated_at = ? "
                        + "WHERE id = ? AND owner_token = ? AND status IN ('INIT','FAILED')",
                toDbTime(sentAt), toDbTime(sentAt), id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean markFailed(Long id, String ownerToken, String error,
                              Instant nextRetryTime, int retryCount,
                              boolean dead, Instant now) {
        if (dead) {
            int rows = jdbcTemplate.update(
                    "UPDATE " + tableName + " SET status = 'DEAD', owner_token = NULL, owner_expire_at = NULL, "
                            + "retry_count = ?, next_retry_time = NULL, last_error = ?, updated_at = ? "
                            + "WHERE id = ? AND owner_token = ? AND status IN ('INIT','FAILED')",
                    retryCount, error, toDbTime(now), id, ownerToken);
            return rows == 1;
        }
        int rows = jdbcTemplate.update(
                "UPDATE " + tableName + " SET status = 'FAILED', owner_token = NULL, owner_expire_at = NULL, "
                        + "retry_count = ?, next_retry_time = ?, last_error = ?, updated_at = ? "
                        + "WHERE id = ? AND owner_token = ? AND status IN ('INIT','FAILED')",
                retryCount, toDbTime(nextRetryTime), error, toDbTime(now), id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean resetForReplay(String msgId, Instant nextRetryTime, Instant now) {
        int rows = jdbcTemplate.update(
                "UPDATE " + tableName + " SET status = 'INIT', retry_count = 0, "
                        + "next_retry_time = ?, "
                        + "owner_token = NULL, owner_expire_at = NULL, last_error = NULL, updated_at = ? "
                        + "WHERE msg_id = ? AND status = 'DEAD' AND deleted = 0",
                toDbTime(nextRetryTime), toDbTime(now), msgId);
        return rows == 1;
    }

    @Override
    public List<Long> scanDispatchableIds(int limit, Instant now) {
        return jdbcTemplate.queryForList(
                "SELECT id FROM " + tableName + " "
                        + "WHERE status IN ('INIT', 'FAILED') AND deleted = 0 "
                        + "AND next_retry_time IS NOT NULL "
                        + "AND next_retry_time <= ? "
                        + "AND (owner_token IS NULL OR owner_expire_at < ?) "
                        + "ORDER BY next_retry_time, id LIMIT ?",
                Long.class, toDbTime(now), toDbTime(now), limit);
    }

    @Override
    public List<Long> scanSentBefore(Instant before, int limit) {
        return jdbcTemplate.queryForList(
                "SELECT id FROM " + tableName + " WHERE status = 'SENT' AND sent_at < ? "
                        + "ORDER BY sent_at LIMIT ?",
                Long.class, toDbTime(before), limit);
    }

    @Override
    public int deleteSent(List<Long> ids) {
        if (ids.isEmpty()) {
            return 0;
        }
        String inSql = String.join(",", Collections.nCopies(ids.size(), "?"));
        return jdbcTemplate.update(
                "DELETE FROM " + tableName + " WHERE id IN (" + inSql + ") AND status = 'SENT'",
                ids.toArray());
    }
}
```

- [ ] **Step 5: Create the canonical MySQL DDL resource**

`reliable-outbox-core/src/main/resources/ddl/mysql/t_outbox.sql`:

```sql
CREATE TABLE `t_outbox` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键 ID，自增',
  `msg_id` VARCHAR(64) NOT NULL COMMENT '消息唯一 ID，全局唯一',
  `biz_code` VARCHAR(64) NOT NULL COMMENT '业务编码，用于区分不同业务方或发送场景',
  `topic` VARCHAR(255) NOT NULL COMMENT '消息主题',
  `partition_key` VARCHAR(255) DEFAULT NULL COMMENT '分区键，用于 Broker 分区选择与消费分组，不承诺顺序',
  `payload` TEXT NOT NULL COMMENT '消息体，默认上限 65535 字节',
  `headers` TEXT DEFAULT NULL COMMENT '透传元数据，默认上限 4096 字节',
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

- [ ] **Step 6: Run tests to verify they pass**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=JdbcOutboxRepositoryTest`
Expected: BUILD SUCCESS, all repository tests pass.

- [ ] **Step 7: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/repository reliable-outbox-core/src/main/resources/ddl reliable-outbox-core/src/test/resources/schema-h2.sql reliable-outbox-core/src/test/java/com/lazycece/outbox/repository
git commit -m "feat: add outbox repository and JDBC implementation"
```

---

### Task 5: Transaction executor

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/transaction/TransactionExecutor.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/transaction/SpringTransactionExecutor.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/transaction/SpringTransactionExecutorTest.java`

**Interfaces:**
- Consumes: `PlatformTransactionManager`.
- Produces: `TransactionExecutor.executeInExistingTransaction(Supplier<T>)`, `executeInNewTransaction(Supplier<T>)`, `afterCommit(Runnable)`; `SpringTransactionExecutor(PlatformTransactionManager)`.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

class SpringTransactionExecutorTest {

    private JdbcTemplate jdbcTemplate;
    private TransactionTemplate transactionTemplate;
    private SpringTransactionExecutor executor;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("CREATE TABLE biz_order (id BIGINT PRIMARY KEY)");
        jdbcTemplate.execute("CREATE TABLE biz_event (id BIGINT PRIMARY KEY)");
        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(transactionManager);
        executor = new SpringTransactionExecutor(transactionManager);
    }

    @Test
    void mandatoryFailsWithoutAnExistingTransaction() {
        assertThatThrownBy(() -> executor.executeInExistingTransaction(() -> "x"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void mandatoryJoinsTheExistingTransaction() {
        AtomicBoolean joined = new AtomicBoolean();
        transactionTemplate.executeWithoutResult(status -> {
            executor.executeInExistingTransaction(() -> {
                joined.set(true);
                return null;
            });
        });
        assertThat(joined).isTrue();
    }

    @Test
    void requiresNewSurvivesOuterTransactionRollback() {
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update("INSERT INTO biz_order (id) VALUES (1)");
            executor.executeInNewTransaction(() -> {
                jdbcTemplate.update("INSERT INTO biz_event (id) VALUES (1)");
                return null;
            });
            status.setRollbackOnly();
        });

        assertThat(count("biz_order")).isZero();
        assertThat(count("biz_event")).isEqualTo(1);
    }

    @Test
    void afterCommitRunsOnlyAfterCommit() {
        AtomicBoolean callback = new AtomicBoolean();
        transactionTemplate.executeWithoutResult(status -> {
            executor.afterCommit(() -> callback.set(true));
            assertThat(callback).isFalse();
        });
        assertThat(callback).isTrue();
    }

    @Test
    void afterCommitDoesNotRunOnRollback() {
        AtomicBoolean callback = new AtomicBoolean();
        transactionTemplate.executeWithoutResult(status -> {
            executor.afterCommit(() -> callback.set(true));
            status.setRollbackOnly();
        });
        assertThat(callback).isFalse();
    }

    @Test
    void afterCommitRequiresAnActiveTransaction() {
        assertThatThrownBy(() -> executor.afterCommit(() -> { }))
                .isInstanceOf(IllegalStateException.class);
    }

    private int count(String table) {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=SpringTransactionExecutorTest`
Expected: compilation failure — `TransactionExecutor` / `SpringTransactionExecutor` do not exist.

- [ ] **Step 3: Implement the transaction executor**

`TransactionExecutor.java`:

```java
package com.lazycece.outbox.transaction;

import java.util.function.Supplier;

public interface TransactionExecutor {
    /** Joins an existing transaction on the outbox data source; fails when none exists. */
    <T> T executeInExistingTransaction(Supplier<T> action);

    /** Suspends any current transaction and creates an independent one. */
    <T> T executeInNewTransaction(Supplier<T> action);

    /** Must be called while one of the transactions above is active. */
    void afterCommit(Runnable action);
}
```

`SpringTransactionExecutor.java`:

```java
package com.lazycece.outbox.transaction;

import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

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
                    log.error("reliable-outbox afterCommit action failed", ex);
                }
            }
        });
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=SpringTransactionExecutorTest`
Expected: BUILD SUCCESS, 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/transaction reliable-outbox-core/src/test/java/com/lazycece/outbox/transaction
git commit -m "feat: add transaction executor with MANDATORY and REQUIRES_NEW semantics"
```

---

### Task 6: Message client and size validation

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/client/MessageClient.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/client/MessageTooLargeException.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/client/DefaultMessageClient.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/client/DefaultMessageClientTest.java`

**Interfaces:**
- Consumes: `OutboxRepository`, `MessageDispatcher.dispatchById(Long)`, `TransactionExecutor`, `ReliableOutboxProperties`, `ExecutorService`.
- Produces: `MessageClient.sendInTransaction(OutboxMessage): String`, `sendStandalone(OutboxMessage): String`; `MessageTooLargeException(String field, int actualBytes, int maxBytes)` with getters.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.transaction.TransactionExecutor;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultMessageClientTest {

    private OutboxRepository repository;
    private MessageDispatcher dispatcher;
    private ExecutorService dispatchExecutor;
    private ImmediateTransactionExecutor transactionExecutor;
    private ReliableOutboxProperties properties;
    private DefaultMessageClient client;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxRepository.class);
        dispatcher = mock(MessageDispatcher.class);
        dispatchExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(dispatchExecutor).execute(any());
        transactionExecutor = new ImmediateTransactionExecutor();
        properties = new ReliableOutboxProperties();
        client = new DefaultMessageClient(repository, dispatcher, transactionExecutor,
                dispatchExecutor, properties);
        when(repository.insertIfAbsent(any())).thenAnswer(invocation -> {
            ((OutboxMessage) invocation.getArgument(0)).setId(7L);
            return true;
        });
    }

    @Test
    void sendInTransactionPreparesPersistsAndDispatchesAfterCommit() {
        OutboxMessage message = message();

        String msgId = client.sendInTransaction(message);

        assertThat(msgId).isEqualTo(message.getMsgId()).hasSize(32);
        assertThat(transactionExecutor.usedExistingTransaction).isTrue();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(message.getRetryCount()).isZero();
        assertThat(message.getCreatedAt()).isNotNull();
        assertThat(message.getNextRetryTime())
                .isEqualTo(message.getCreatedAt().plus(Duration.ofSeconds(30)));
        verify(repository).insertIfAbsent(message);
        verify(dispatcher).dispatchById(7L);
    }

    @Test
    void sendStandaloneUsesNewTransaction() {
        client.sendStandalone(message());

        assertThat(transactionExecutor.usedExistingTransaction).isFalse();
        verify(dispatcher).dispatchById(7L);
    }

    @Test
    void duplicateMsgIdDoesNotRegisterDispatch() {
        when(repository.insertIfAbsent(any())).thenReturn(false);
        when(repository.findByMsgId("dup")).thenReturn(new OutboxMessage());
        OutboxMessage message = message();
        message.setMsgId("dup");

        String result = client.sendInTransaction(message);

        assertThat(result).isEqualTo("dup");
        verify(dispatcher, never()).dispatchById(anyLong());
        assertThat(transactionExecutor.callbacks).isEmpty();
    }

    @Test
    void payloadAtLimitIsAccepted() {
        OutboxMessage message = message();
        message.setPayload("a".repeat(65535));

        client.sendInTransaction(message);

        verify(repository).insertIfAbsent(message);
    }

    @Test
    void payloadOverLimitIsRejectedBeforeAnyDatabaseWork() {
        OutboxMessage message = message();
        message.setPayload("a".repeat(65536));

        assertThatThrownBy(() -> client.sendInTransaction(message))
                .isInstanceOf(MessageTooLargeException.class)
                .hasMessageContaining("65536")
                .hasMessageContaining("65535");
        verifyNoInteractions(repository);
    }

    @Test
    void headersAtLimitAreAcceptedAndOverLimitRejected() {
        OutboxMessage ok = message();
        ok.setHeaders("h".repeat(4096));
        client.sendInTransaction(ok);
        verify(repository).insertIfAbsent(ok);

        OutboxMessage tooLarge = message();
        tooLarge.setHeaders("h".repeat(4097));
        assertThatThrownBy(() -> client.sendInTransaction(tooLarge))
                .isInstanceOf(MessageTooLargeException.class)
                .hasMessageContaining("headers");
    }

    @Test
    void multiByteCharactersAreCountedByUtf8Bytes() {
        properties.setMaxPayloadBytes(6);
        OutboxMessage ok = message();
        ok.setPayload("中文");
        client.sendInTransaction(ok);
        verify(repository).insertIfAbsent(ok);

        OutboxMessage tooLarge = message();
        tooLarge.setPayload("中文a");
        assertThatThrownBy(() -> client.sendInTransaction(tooLarge))
                .isInstanceOf(MessageTooLargeException.class)
                .hasMessageContaining("actual=7");
    }

    @Test
    void rejectsInvalidFieldsAndMsgId() {
        OutboxMessage noBiz = message();
        noBiz.setBizCode(" ");
        assertThatThrownBy(() -> client.sendInTransaction(noBiz))
                .isInstanceOf(IllegalArgumentException.class);

        OutboxMessage badMsgId = message();
        badMsgId.setMsgId("bad id!");
        assertThatThrownBy(() -> client.sendInTransaction(badMsgId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("msgId");
    }

    private static OutboxMessage message() {
        OutboxMessage message = new OutboxMessage();
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPartitionKey("order-1");
        message.setPayload("{}");
        return message;
    }

    private static final class ImmediateTransactionExecutor implements TransactionExecutor {
        private final List<Runnable> callbacks = new ArrayList<>();
        private Boolean usedExistingTransaction;

        @Override
        public <T> T executeInExistingTransaction(Supplier<T> action) {
            usedExistingTransaction = true;
            return run(action);
        }

        @Override
        public <T> T executeInNewTransaction(Supplier<T> action) {
            usedExistingTransaction = false;
            return run(action);
        }

        private <T> T run(Supplier<T> action) {
            T result = action.get();
            List<Runnable> pending = new ArrayList<>(callbacks);
            callbacks.clear();
            pending.forEach(Runnable::run);
            return result;
        }

        @Override
        public void afterCommit(Runnable action) {
            callbacks.add(action);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=DefaultMessageClientTest`
Expected: compilation failure — `DefaultMessageClient` does not exist.

- [ ] **Step 3: Implement the client**

`MessageClient.java`:

```java
package com.lazycece.outbox.client;

import com.lazycece.outbox.model.OutboxMessage;

public interface MessageClient {
    /** Must join an existing business transaction. */
    String sendInTransaction(OutboxMessage message);

    /** Uses an independent REQUIRES_NEW transaction. */
    String sendStandalone(OutboxMessage message);
}
```

`MessageTooLargeException.java`:

```java
package com.lazycece.outbox.client;

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

    public String getField() {
        return field;
    }

    public int getActualBytes() {
        return actualBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }
}
```

`DefaultMessageClient.java`:

```java
package com.lazycece.outbox.client;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.transaction.TransactionExecutor;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            case IN_TRANSACTION -> transactionExecutor.executeInExistingTransaction(persistAction);
            case STANDALONE -> transactionExecutor.executeInNewTransaction(persistAction);
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
            throw new MessageTooLargeException("payload", payloadBytes, properties.getMaxPayloadBytes());
        }
        if (message.getHeaders() != null) {
            int headerBytes = message.getHeaders().getBytes(StandardCharsets.UTF_8).length;
            if (headerBytes > properties.getMaxHeadersBytes()) {
                throw new MessageTooLargeException("headers", headerBytes, properties.getMaxHeadersBytes());
            }
        }
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " 不能为空");
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=DefaultMessageClientTest`
Expected: BUILD SUCCESS. If `MessageDispatcher` does not exist yet, temporarily compile against a stub interface is NOT allowed — run Task 9 first if compilation is blocked. The recommended order is to complete Task 9 (dispatcher) before this step; the task numbering keeps client before dispatcher only for narrative order.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/client reliable-outbox-core/src/test/java/com/lazycece/outbox/client
git commit -m "feat: add message client with transaction semantics and size validation"
```

---

### Task 7: Retry policy

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/RetryPolicy.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/dispatch/RetryPolicyTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `RetryPolicy(int maxRetryCount, Duration initialInterval, Duration maxInterval, Duration maxJitter)`, `boolean isDead(int retryCount)`, `Instant nextRetryTime(int retryCount)`.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    @Test
    void marksDeadAtMaxRetryCount() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);
        assertThat(policy.isDead(15)).isFalse();
        assertThat(policy.isDead(16)).isTrue();
    }

    @Test
    void backsOffExponentiallyAndCapsAtMaxInterval() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);

        assertThat(delayMillis(policy.nextRetryTime(1))).isBetween(4900L, 5100L);
        assertThat(delayMillis(policy.nextRetryTime(2))).isBetween(9900L, 10100L);
        assertThat(delayMillis(policy.nextRetryTime(7))).isBetween(299900L, 300100L);
        assertThat(delayMillis(policy.nextRetryTime(16))).isBetween(299900L, 300100L);
    }

    @Test
    void zeroJitterDoesNotThrow() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);
        assertThat(policy.nextRetryTime(1)).isAfter(Instant.now());
    }

    @Test
    void jitterStaysWithinConfiguredBound() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ofMillis(1000));
        long delay = delayMillis(policy.nextRetryTime(1));
        assertThat(delay).isBetween(5000L, 6000L);
    }

    private static long delayMillis(Instant next) {
        return Duration.between(Instant.now(), next).toMillis();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=RetryPolicyTest`
Expected: compilation failure — `RetryPolicy` does not exist.

- [ ] **Step 3: Implement `RetryPolicy`**

```java
package com.lazycece.outbox.dispatch;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

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
        long baseMillis = Math.min(initialInterval.toMillis() * (1L << shift),
                maxInterval.toMillis());
        long jitterMillis = maxJitterMillis > 0
                ? ThreadLocalRandom.current().nextLong(maxJitterMillis)
                : 0L;
        return Instant.now().plusMillis(baseMillis + jitterMillis);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=RetryPolicyTest`
Expected: BUILD SUCCESS, 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/RetryPolicy.java reliable-outbox-core/src/test/java/com/lazycece/outbox/dispatch/RetryPolicyTest.java
git commit -m "feat: add exponential backoff retry policy"
```

---

### Task 8: Retry scheduler

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/RetryScheduler.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/DefaultRetryScheduler.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/MessageDispatcher.java` (interface stub for this task; full behavior in Task 9)
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/dispatch/DefaultRetrySchedulerTest.java`

**Interfaces:**
- Consumes: `ObjectProvider<MessageDispatcher>`, `Executor`, `ScheduledExecutorService`.
- Produces: `RetryScheduler.schedule(Long messageId, long delayMillis)`; `DefaultRetryScheduler(ObjectProvider<MessageDispatcher>, Executor, ScheduledExecutorService)`.

- [ ] **Step 1: Create the `MessageDispatcher` stub used to compile this task**

This file is replaced with the full implementation in Task 9.

```java
package com.lazycece.outbox.dispatch;

public class MessageDispatcher {
    public void dispatchById(Long id) {
    }

    public void dispatchDue(Long id) {
    }

    public void dispatchAcquired(Long id, String ownerToken) {
    }

    public void retry(Long id) {
    }
}
```

- [ ] **Step 2: Write the failing test**

```java
package com.lazycece.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

class DefaultRetrySchedulerTest {

    @Test
    void schedulesAndSubmitsRetryToDispatchExecutor() {
        @SuppressWarnings("unchecked")
        ObjectProvider<MessageDispatcher> provider = mock(ObjectProvider.class);
        MessageDispatcher dispatcher = mock(MessageDispatcher.class);
        when(provider.getObject()).thenReturn(dispatcher);
        Executor executor = mock(Executor.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(any());
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        DefaultRetryScheduler retryScheduler = new DefaultRetryScheduler(provider, executor, scheduler);

        retryScheduler.schedule(42L, 1500L);

        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(task.capture(), org.mockito.ArgumentMatchers.eq(1500L),
                org.mockito.ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
        task.getValue().run();
        verify(dispatcher).retry(42L);
    }

    @Test
    void rejectedRetryDoesNotPropagate() {
        @SuppressWarnings("unchecked")
        ObjectProvider<MessageDispatcher> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(mock(MessageDispatcher.class));
        Executor executor = mock(Executor.class);
        doThrow(new RejectedExecutionException("full")).when(executor).execute(any());
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        DefaultRetryScheduler retryScheduler = new DefaultRetryScheduler(provider, executor, scheduler);

        retryScheduler.schedule(42L, 0L);

        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(task.capture(), anyLong(), any(TimeUnit.class));
        assertThatCode(() -> task.getValue().run()).doesNotThrowAnyException();
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=DefaultRetrySchedulerTest`
Expected: compilation failure — `RetryScheduler` / `DefaultRetryScheduler` do not exist.

- [ ] **Step 4: Implement the retry scheduler**

`RetryScheduler.java`:

```java
package com.lazycece.outbox.dispatch;

public interface RetryScheduler {
    void schedule(Long messageId, long delayMillis);
}
```

`DefaultRetryScheduler.java`:

```java
package com.lazycece.outbox.dispatch;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

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
                log.warn("retry dispatch rejected, id={} will be retried by scanner", messageId);
            }
        }, Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=DefaultRetrySchedulerTest`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch reliable-outbox-core/src/test/java/com/lazycece/outbox/dispatch/DefaultRetrySchedulerTest.java
git commit -m "feat: add retry scheduler accelerator"
```

---

### Task 9: Message dispatcher

**Files:**
- Modify: `reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/MessageDispatcher.java`
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/deadletter/DeadLetterHandler.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/support/InMemoryOutboxRepository.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/dispatch/MessageDispatcherTest.java`

**Interfaces:**
- Consumes: `OutboxRepository`, `MessagePublisher`, `RetryPolicy`, `RetryScheduler`, `DeadLetterHandler.onDead(OutboxMessage)`, `MessageListener`, `Duration ownerLease`, `Duration publishTimeout`.
- Produces: `MessageDispatcher.dispatchById(Long)`, `dispatchDue(Long)`, `dispatchAcquired(Long, String)`, `retry(Long)`.

- [ ] **Step 1: Create the test-support in-memory repository**

`InMemoryOutboxRepository.java`:

```java
package com.lazycece.outbox.support;

import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.repository.OutboxRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryOutboxRepository implements OutboxRepository {
    private final Map<Long, OutboxMessage> messages = new LinkedHashMap<>();
    private final AtomicLong sequence = new AtomicLong(1);

    public synchronized OutboxMessage put(OutboxMessage message) {
        OutboxMessage copy = copy(message);
        if (copy.getId() == null) {
            copy.setId(sequence.getAndIncrement());
        }
        messages.put(copy.getId(), copy);
        return copy(copy);
    }

    public synchronized void setDeleted(Long id, boolean deleted) {
        OutboxMessage message = messages.get(id);
        Objects.requireNonNull(message, "message");
        message.setDeleted(deleted);
    }

    public synchronized OutboxMessage get(Long id) {
        OutboxMessage message = messages.get(id);
        return message == null ? null : copy(message);
    }

    public synchronized List<OutboxMessage> all() {
        return messages.values().stream().map(this::copy).toList();
    }

    @Override
    public synchronized boolean insertIfAbsent(OutboxMessage message) {
        if (findByMsgIdInternal(message.getMsgId()) != null) {
            return false;
        }
        OutboxMessage copy = copy(message);
        copy.setId(sequence.getAndIncrement());
        messages.put(copy.getId(), copy);
        message.setId(copy.getId());
        return true;
    }

    @Override
    public synchronized OutboxMessage findById(Long id) {
        OutboxMessage message = messages.get(id);
        return message == null ? null : copy(message);
    }

    @Override
    public synchronized OutboxMessage findByMsgId(String msgId) {
        OutboxMessage message = findByMsgIdInternal(msgId);
        return message == null ? null : copy(message);
    }

    private OutboxMessage findByMsgIdInternal(String msgId) {
        return messages.values().stream()
                .filter(message -> Objects.equals(message.getMsgId(), msgId))
                .findFirst().orElse(null);
    }

    @Override
    public synchronized boolean tryAcquire(Long id, String ownerToken, Instant ownerExpireAt, Instant now) {
        OutboxMessage message = messages.get(id);
        if (!acquirable(message, now)) {
            return false;
        }
        message.setOwnerToken(ownerToken);
        message.setOwnerExpireAt(ownerExpireAt);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized boolean tryAcquireDue(Long id, String ownerToken, Instant ownerExpireAt, Instant dueAt) {
        OutboxMessage message = messages.get(id);
        if (!acquirable(message, dueAt)) {
            return false;
        }
        if (message.getNextRetryTime() == null || message.getNextRetryTime().isAfter(dueAt)) {
            return false;
        }
        message.setOwnerToken(ownerToken);
        message.setOwnerExpireAt(ownerExpireAt);
        message.setUpdatedAt(dueAt);
        return true;
    }

    private boolean acquirable(OutboxMessage message, Instant now) {
        if (message == null || !message.getStatus().isDispatchable() || message.isDeleted()) {
            return false;
        }
        return message.getOwnerToken() == null
                || message.getOwnerExpireAt() == null
                || message.getOwnerExpireAt().isBefore(now);
    }

    @Override
    public synchronized boolean releaseLease(Long id, String ownerToken, Instant now) {
        OutboxMessage message = messages.get(id);
        if (message == null || !Objects.equals(message.getOwnerToken(), ownerToken)) {
            return false;
        }
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized boolean markSent(Long id, String ownerToken, Instant sentAt) {
        OutboxMessage message = messages.get(id);
        if (message == null || !Objects.equals(message.getOwnerToken(), ownerToken)
                || !message.getStatus().isDispatchable()) {
            return false;
        }
        message.setStatus(com.lazycece.outbox.model.OutboxStatus.SENT);
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setSentAt(sentAt);
        message.setNextRetryTime(null);
        message.setLastError(null);
        message.setUpdatedAt(sentAt);
        return true;
    }

    @Override
    public synchronized boolean markFailed(Long id, String ownerToken, String error,
                                           Instant nextRetryTime, int retryCount,
                                           boolean dead, Instant now) {
        OutboxMessage message = messages.get(id);
        if (message == null || !Objects.equals(message.getOwnerToken(), ownerToken)
                || !message.getStatus().isDispatchable()) {
            return false;
        }
        message.setStatus(dead
                ? com.lazycece.outbox.model.OutboxStatus.DEAD
                : com.lazycece.outbox.model.OutboxStatus.FAILED);
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setRetryCount(retryCount);
        message.setNextRetryTime(dead ? null : nextRetryTime);
        message.setLastError(error);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized boolean resetForReplay(String msgId, Instant nextRetryTime, Instant now) {
        OutboxMessage message = findByMsgIdInternal(msgId);
        if (message == null || message.getStatus() != com.lazycece.outbox.model.OutboxStatus.DEAD
                || message.isDeleted()) {
            return false;
        }
        message.setStatus(com.lazycece.outbox.model.OutboxStatus.INIT);
        message.setRetryCount(0);
        message.setNextRetryTime(nextRetryTime);
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setLastError(null);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized List<Long> scanDispatchableIds(int limit, Instant now) {
        List<OutboxMessage> candidates = new ArrayList<>();
        for (OutboxMessage message : messages.values()) {
            if (message.getStatus().isDispatchable() && !message.isDeleted()
                    && message.getNextRetryTime() != null
                    && !message.getNextRetryTime().isAfter(now)
                    && acquirable(message, now)) {
                candidates.add(message);
            }
        }
        candidates.sort(Comparator
                .comparing(OutboxMessage::getNextRetryTime)
                .thenComparing(OutboxMessage::getId));
        return candidates.stream().limit(limit).map(OutboxMessage::getId).toList();
    }

    @Override
    public synchronized List<Long> scanSentBefore(Instant before, int limit) {
        return messages.values().stream()
                .filter(message -> message.getStatus() == com.lazycece.outbox.model.OutboxStatus.SENT)
                .filter(message -> message.getSentAt() != null && message.getSentAt().isBefore(before))
                .sorted(Comparator.comparing(OutboxMessage::getSentAt))
                .limit(limit)
                .map(OutboxMessage::getId)
                .toList();
    }

    @Override
    public synchronized int deleteSent(List<Long> ids) {
        int deleted = 0;
        for (Long id : ids) {
            OutboxMessage message = messages.get(id);
            if (message != null && message.getStatus() == com.lazycece.outbox.model.OutboxStatus.SENT) {
                messages.remove(id);
                deleted++;
            }
        }
        return deleted;
    }

    private OutboxMessage copy(OutboxMessage source) {
        OutboxMessage copy = new OutboxMessage();
        copy.setId(source.getId());
        copy.setMsgId(source.getMsgId());
        copy.setBizCode(source.getBizCode());
        copy.setTopic(source.getTopic());
        copy.setPartitionKey(source.getPartitionKey());
        copy.setPayload(source.getPayload());
        copy.setHeaders(source.getHeaders());
        copy.setStatus(source.getStatus());
        copy.setDeleted(source.isDeleted());
        copy.setRetryCount(source.getRetryCount());
        copy.setNextRetryTime(source.getNextRetryTime());
        copy.setOwnerToken(source.getOwnerToken());
        copy.setOwnerExpireAt(source.getOwnerExpireAt());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        copy.setSentAt(source.getSentAt());
        copy.setLastError(source.getLastError());
        return copy;
    }
}
```

- [ ] **Step 2: Write the failing dispatcher test**

```java
package com.lazycece.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.deadletter.DeadLetterHandler;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.spi.MessageListener;
import com.lazycece.outbox.spi.MessagePublisher;
import com.lazycece.outbox.spi.NonRetryablePublishException;
import com.lazycece.outbox.spi.PublishException;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MessageDispatcherTest {

    private InMemoryOutboxRepository repository;
    private MessagePublisher publisher;
    private RetryScheduler retryScheduler;
    private MessageListener listener;
    private DeadLetterHandler deadLetterHandler;
    private MessageDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        publisher = mock(MessagePublisher.class);
        retryScheduler = mock(RetryScheduler.class);
        listener = mock(MessageListener.class);
        deadLetterHandler = mock(DeadLetterHandler.class);
        RetryPolicy retryPolicy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);
        dispatcher = new MessageDispatcher(repository, publisher, retryPolicy, retryScheduler,
                deadLetterHandler, listener, Duration.ofSeconds(60), Duration.ofSeconds(20));
    }

    private OutboxMessage seed(String msgId, OutboxStatus status, int retryCount) {
        Instant now = Instant.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(status);
        message.setRetryCount(retryCount);
        message.setNextRetryTime(status == OutboxStatus.FAILED
                ? now.minusSeconds(1) : now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void successfulPublishMarksSentAndNotifies() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(stored.getOwnerToken()).isNull();
        verify(publisher).publish(any(OutboxMessage.class));
        verify(listener).onSent(any(OutboxMessage.class));
    }

    @Test
    void retryableFailureSchedulesRetry() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        doThrow(new PublishException("boom", null)).when(publisher).publish(any());

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(stored.getRetryCount()).isEqualTo(1);
        assertThat(stored.getNextRetryTime()).isAfter(Instant.now());
        assertThat(stored.getOwnerToken()).isNull();
        assertThat(stored.getLastError()).isEqualTo("boom");
        verify(listener).onFailed(any(OutboxMessage.class), eq(1), any(PublishException.class));
        verify(retryScheduler).schedule(eq(message.getId()), anyLong());
    }

    @Test
    void nonRetryableFailureMarksDead() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        doThrow(new NonRetryablePublishException("bad topic", null)).when(publisher).publish(any());

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(stored.getNextRetryTime()).isNull();
        assertThat(stored.getRetryCount()).isEqualTo(1);
        verify(deadLetterHandler).onDead(any(OutboxMessage.class));
        verify(retryScheduler, never()).schedule(anyLong(), anyLong());
    }

    @Test
    void failureAtMaxRetryCountMarksDead() {
        OutboxMessage message = seed("m1", OutboxStatus.FAILED, 15);
        doThrow(new PublishException("boom", null)).when(publisher).publish(any());

        dispatcher.dispatchDue(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(stored.getRetryCount()).isEqualTo(16);
        verify(deadLetterHandler).onDead(any(OutboxMessage.class));
    }

    @Test
    void insufficientLeaseIsReleasedWithoutPublishing() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        Instant now = Instant.now();
        repository.tryAcquire(message.getId(), "t", now.plusSeconds(5), now);

        dispatcher.dispatchAcquired(message.getId(), "t");

        verify(publisher, never()).publish(any());
        assertThat(repository.get(message.getId()).getOwnerToken()).isNull();
    }

    @Test
    void staleOwnerIsSkippedAndReported() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        Instant now = Instant.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);

        dispatcher.dispatchAcquired(message.getId(), "other");

        verify(publisher, never()).publish(any());
        verify(listener).onLeaseLost(any(OutboxMessage.class), eq("lease-lost-before-publish"));
    }

    @Test
    void deletedMessageIsNotPublished() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        Instant now = Instant.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);
        repository.setDeleted(message.getId(), true);

        dispatcher.dispatchAcquired(message.getId(), "owner");

        verify(publisher, never()).publish(any());
    }

    @Test
    void markSentFailureReportsLeaseLoss() {
        InMemoryOutboxRepository spyRepository = spy(repository);
        OutboxMessage message = spyRepository.put(seedMessage("m1"));
        doReturn(false).when(spyRepository).markSent(anyLong(), any(), any());
        MessageDispatcher spyDispatcher = new MessageDispatcher(spyRepository, publisher,
                new RetryPolicy(16, Duration.ofSeconds(5), Duration.ofSeconds(300), Duration.ZERO),
                retryScheduler, deadLetterHandler, listener,
                Duration.ofSeconds(60), Duration.ofSeconds(20));

        spyDispatcher.dispatchById(message.getId());

        verify(publisher).publish(any(OutboxMessage.class));
        verify(listener).onLeaseLost(any(OutboxMessage.class), eq("mark-sent-failed"));
    }

    private OutboxMessage seedMessage(String msgId) {
        Instant now = Instant.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(OutboxStatus.INIT);
        message.setNextRetryTime(now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return message;
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=MessageDispatcherTest`
Expected: test failures or compilation failure because the dispatcher is still a stub.

- [ ] **Step 4: Implement `DeadLetterHandler` and the full dispatcher**

`DeadLetterHandler.java`:

```java
package com.lazycece.outbox.deadletter;

import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.spi.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
```

`MessageDispatcher.java` (replace the stub):

```java
package com.lazycece.outbox.dispatch;

import com.lazycece.outbox.deadletter.DeadLetterHandler;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.spi.MessageListener;
import com.lazycece.outbox.spi.MessagePublisher;
import com.lazycece.outbox.spi.NonRetryablePublishException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** The caller must already hold the lease for this message. */
    public void dispatchAcquired(Long id, String ownerToken) {
        OutboxMessage fresh = repository.findById(id);
        if (fresh == null) {
            log.warn("outbox message not found, id={}", id);
            return;
        }
        if (fresh.isDeleted()) {
            log.debug("outbox message deleted, skip, id={}, msgId={}", id, fresh.getMsgId());
            return;
        }
        Instant now = Instant.now();
        if (!ownerToken.equals(fresh.getOwnerToken())) {
            log.warn("lease lost before publish, skip, id={}, msgId={}", id, fresh.getMsgId());
            safeNotify(() -> listener.onLeaseLost(fresh, "lease-lost-before-publish"));
            return;
        }
        if (fresh.getOwnerExpireAt() == null
                || !fresh.getOwnerExpireAt().isAfter(now.plus(publishTimeout))) {
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

        message.setRetryCount(retryCount);
        message.setLastError(errorMessage);
        message.setStatus(dead ? OutboxStatus.DEAD : OutboxStatus.FAILED);
        message.setNextRetryTime(nextRetryTime);

        if (dead) {
            deadLetterHandler.onDead(message);
            return;
        }

        safeNotify(() -> listener.onFailed(message, retryCount, error));
        long delayMillis = Math.max(0, Duration.between(Instant.now(), nextRetryTime).toMillis());
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
        if (error == null) {
            return null;
        }
        return error.length() > 1000 ? error.substring(0, 1000) : error;
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=MessageDispatcherTest`
Expected: BUILD SUCCESS, 8 tests pass.

- [ ] **Step 6: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/dispatch/MessageDispatcher.java reliable-outbox-core/src/main/java/com/lazycece/outbox/deadletter/DeadLetterHandler.java reliable-outbox-core/src/test/java/com/lazycece/outbox/support reliable-outbox-core/src/test/java/com/lazycece/outbox/dispatch/MessageDispatcherTest.java
git commit -m "feat: add message dispatcher with lease-based delivery"
```

---

### Task 10: Message scanner

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/scan/MessageScanner.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/scan/MessageScannerTest.java`

**Interfaces:**
- Consumes: `OutboxRepository.scanDispatchableIds` / `tryAcquireDue` / `releaseLease`, `MessageDispatcher.dispatchAcquired`, `ExecutorService`, `ScheduledExecutorService`, `ReliableOutboxProperties`.
- Produces: `MessageScanner.start()` (`@PostConstruct`, schedules `scanSafely`), `MessageScanner.scan()`.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.scan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MessageScannerTest {

    private InMemoryOutboxRepository repository;
    private MessageDispatcher dispatcher;
    private ExecutorService executor;
    private ReliableOutboxProperties properties;
    private MessageScanner scanner;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        dispatcher = mock(MessageDispatcher.class);
        executor = mock(ExecutorService.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        properties = new ReliableOutboxProperties();
        properties.setScanBatchSize(2);
        properties.setMaxScanBatchesPerRun(10);
        scanner = new MessageScanner(repository, dispatcher, executor, scheduler, properties);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(any());
    }

    private OutboxMessage seed(String msgId, Instant nextRetry) {
        Instant now = Instant.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(OutboxStatus.INIT);
        message.setNextRetryTime(nextRetry);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void scansAllDueMessagesAcrossBatchesAndSkipsFutureOnes() {
        seed("due1", Instant.now().minusSeconds(10));
        seed("due2", Instant.now().minusSeconds(5));
        seed("due3", Instant.now().minusSeconds(1));
        seed("future", Instant.now().plusSeconds(60));

        scanner.scan();

        verify(dispatcher, org.mockito.Mockito.times(3))
                .dispatchAcquired(anyLong(), anyString());
    }

    @Test
    void rejectionReleasesTheLease() {
        OutboxMessage message = seed("due1", Instant.now().minusSeconds(1));
        doThrow(new RejectedExecutionException("full")).when(executor).execute(any());

        scanner.scan();

        assertThat(repository.get(message.getId()).getOwnerToken()).isNull();
        verify(dispatcher, never()).dispatchAcquired(anyLong(), anyString());
    }

    @Test
    void stopsWhenNothingIsDue() {
        scanner.scan();
        verify(dispatcher, never()).dispatchAcquired(anyLong(), anyString());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=MessageScannerTest`
Expected: compilation failure — `MessageScanner` does not exist.

- [ ] **Step 3: Implement `MessageScanner`**

```java
package com.lazycece.outbox.scan;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.repository.OutboxRepository;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=MessageScannerTest`
Expected: BUILD SUCCESS, 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/scan reliable-outbox-core/src/test/java/com/lazycece/outbox/scan
git commit -m "feat: add due-message scanner as delivery fallback"
```

---

### Task 11: Outbox cleaner

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/clean/OutboxCleaner.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/clean/OutboxCleanerTest.java`

**Interfaces:**
- Consumes: `OutboxRepository.scanSentBefore` / `deleteSent`, `ScheduledExecutorService`, `ReliableOutboxProperties`.
- Produces: `OutboxCleaner.start()` (`@PostConstruct`), `OutboxCleaner.clean()`.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.clean;

import static org.assertj.core.api.Assertions.assertThat;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

class OutboxCleanerTest {

    private InMemoryOutboxRepository repository;
    private ReliableOutboxProperties properties;
    private OutboxCleaner cleaner;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        properties = new ReliableOutboxProperties();
        properties.setSentRetention(java.time.Duration.ofDays(7));
        properties.setCleanBatchSize(2);
        properties.setCleanMaxBatchesPerRun(2);
        cleaner = new OutboxCleaner(repository, mock(ScheduledExecutorService.class), properties);
    }

    private OutboxMessage seedSent(String msgId, Instant sentAt) {
        Instant now = Instant.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(OutboxStatus.SENT);
        message.setSentAt(sentAt);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void deletesOldSentMessagesInBatchesButNeverDeadOnes() {
        Instant old = Instant.now().minus(8, ChronoUnit.DAYS);
        seedSent("old1", old);
        seedSent("old2", old.plusSeconds(1));
        seedSent("old3", old.plusSeconds(2));
        seedSent("old4", old.plusSeconds(3));
        seedSent("old5", old.plusSeconds(4));
        seedSent("recent", Instant.now());

        OutboxMessage dead = seedSent("dead", old);
        dead.setStatus(OutboxStatus.DEAD);
        repository.put(dead);

        cleaner.clean();

        assertThat(repository.all()).hasSize(3);
        assertThat(repository.all()).extracting(OutboxMessage::getMsgId)
                .containsExactlyInAnyOrder("old5", "recent", "dead");
    }

    @Test
    void secondRunCleansRemainingBatches() {
        Instant old = Instant.now().minus(8, ChronoUnit.DAYS);
        seedSent("old1", old);
        seedSent("old2", old.plusSeconds(1));
        seedSent("old3", old.plusSeconds(2));

        cleaner.clean();
        assertThat(repository.all()).hasSize(1);

        cleaner.clean();
        assertThat(repository.all()).isEmpty();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=OutboxCleanerTest`
Expected: compilation failure — `OutboxCleaner` does not exist.

- [ ] **Step 3: Implement `OutboxCleaner`**

```java
package com.lazycece.outbox.clean;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.repository.OutboxRepository;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=OutboxCleanerTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/clean reliable-outbox-core/src/test/java/com/lazycece/outbox/clean
git commit -m "feat: add batched SENT message cleaner"
```

---

### Task 12: Dead-letter replay

**Files:**
- Create: `reliable-outbox-core/src/main/java/com/lazycece/outbox/deadletter/DeadLetterReplayer.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/deadletter/DeadLetterHandlerTest.java`
- Test: `reliable-outbox-core/src/test/java/com/lazycece/outbox/deadletter/DeadLetterReplayerTest.java`

**Interfaces:**
- Consumes: `OutboxRepository.resetForReplay` / `findByMsgId`, `MessageDispatcher.dispatchById`, `MessageListener.onDead`.
- Produces: `DeadLetterHandler.onDead(OutboxMessage)` (already implemented in Task 9), `DeadLetterReplayer.replay(String msgId): boolean`.

- [ ] **Step 1: Write the failing tests**

`DeadLetterHandlerTest.java`:

```java
package com.lazycece.outbox.deadletter;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.spi.MessageListener;
import org.junit.jupiter.api.Test;

class DeadLetterHandlerTest {

    @Test
    void listenerFailuresAreSwallowed() {
        MessageListener listener = mock(MessageListener.class);
        doThrow(new RuntimeException("listener down")).when(listener).onDead(any());
        DeadLetterHandler handler = new DeadLetterHandler(listener);

        assertThatCode(() -> handler.onDead(new OutboxMessage())).doesNotThrowAnyException();
        verify(listener).onDead(any(OutboxMessage.class));
    }
}
```

`DeadLetterReplayerTest.java`:

```java
package com.lazycece.outbox.deadletter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeadLetterReplayerTest {

    private InMemoryOutboxRepository repository;
    private MessageDispatcher dispatcher;
    private DeadLetterReplayer replayer;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        dispatcher = mock(MessageDispatcher.class);
        replayer = new DeadLetterReplayer(repository, dispatcher);
    }

    private OutboxMessage seed(String msgId, OutboxStatus status) {
        Instant now = Instant.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(status);
        message.setRetryCount(16);
        message.setNextRetryTime(now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void replayResetsDeadMessageAndTriggersDispatch() {
        OutboxMessage message = seed("m1", OutboxStatus.DEAD);

        assertThat(replayer.replay("m1")).isTrue();

        OutboxMessage reset = repository.get(message.getId());
        assertThat(reset.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(reset.getRetryCount()).isZero();
        assertThat(reset.getNextRetryTime()).isNotNull();
        verify(dispatcher).dispatchById(message.getId());
    }

    @Test
    void replayRejectsMissingOrNonDeadMessages() {
        seed("sent", OutboxStatus.SENT);

        assertThat(replayer.replay("missing")).isFalse();
        assertThat(replayer.replay("sent")).isFalse();
        verify(dispatcher, never()).dispatchById(org.mockito.ArgumentMatchers.anyLong());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=DeadLetterHandlerTest,DeadLetterReplayerTest`
Expected: compilation failure — `DeadLetterReplayer` does not exist.

- [ ] **Step 3: Implement `DeadLetterReplayer`**

```java
package com.lazycece.outbox.deadletter;

import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.repository.OutboxRepository;
import java.time.Instant;

public class DeadLetterReplayer {
    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;

    public DeadLetterReplayer(OutboxRepository repository, MessageDispatcher dispatcher) {
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl reliable-outbox-core test -Dtest=DeadLetterHandlerTest,DeadLetterReplayerTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add reliable-outbox-core/src/main/java/com/lazycece/outbox/deadletter/DeadLetterReplayer.java reliable-outbox-core/src/test/java/com/lazycece/outbox/deadletter
git commit -m "feat: add dead-letter replay"
```

---

### Task 13: Spring Boot auto-configuration

**Files:**
- Create: `reliable-outbox-spring-boot-starter/src/main/java/com/lazycece/outbox/autoconfigure/ReliableOutboxAutoConfiguration.java`
- Create: `reliable-outbox-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `reliable-outbox-spring-boot-starter/src/test/java/com/lazycece/outbox/autoconfigure/ReliableOutboxAutoConfigurationTest.java`

**Interfaces:**
- Consumes: every bean produced in Tasks 2-12.
- Produces: auto-configured `OutboxRepository`, `reliableOutboxDispatchExecutor`, `reliableOutboxScheduler`, `TransactionExecutor`, `RetryPolicy`, `MessageListener`, `DeadLetterHandler`, `MessageDispatcher`, `RetryScheduler`, `MessageClient`, `MessageScanner`, `OutboxCleaner`, `DeadLetterReplayer`.

- [ ] **Step 1: Write the failing test**

```java
package com.lazycece.outbox.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lazycece.outbox.client.MessageClient;
import com.lazycece.outbox.clean.OutboxCleaner;
import com.lazycece.outbox.deadletter.DeadLetterReplayer;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.dispatch.RetryScheduler;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.scan.MessageScanner;
import com.lazycece.outbox.spi.MessagePublisher;
import com.lazycece.outbox.transaction.TransactionExecutor;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

class ReliableOutboxAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReliableOutboxAutoConfiguration.class))
            .withUserConfiguration(TestConfig.class);

    @Test
    void registersAllBeans() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(OutboxRepository.class);
            assertThat(context).hasSingleBean(TransactionExecutor.class);
            assertThat(context).hasSingleBean(RetryScheduler.class);
            assertThat(context).hasSingleBean(MessageDispatcher.class);
            assertThat(context).hasSingleBean(MessageClient.class);
            assertThat(context).hasSingleBean(MessageScanner.class);
            assertThat(context).hasSingleBean(OutboxCleaner.class);
            assertThat(context).hasSingleBean(DeadLetterReplayer.class);
            assertThat(context).hasBean("reliableOutboxDispatchExecutor");
            assertThat(context).hasBean("reliableOutboxScheduler");
        });
    }

    @Test
    void disabledPropertySkipsAutoConfiguration() {
        runner.withPropertyValues("reliable-outbox.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MessageClient.class));
    }

    @Test
    void missingPublisherFailsWithClearMessage() {
        ApplicationContextRunner noPublisher = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ReliableOutboxAutoConfiguration.class))
                .withUserConfiguration(DataSourceConfig.class);
        assertThatThrownBy(() -> noPublisher.run(context -> { }))
                .hasStackTraceContaining("reliable-outbox 需要业务方提供 MessagePublisher bean");
    }

    @Configuration(proxyBeanMethods = false)
    static class DataSourceConfig {
        @Bean
        DataSource dataSource() {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:autoconfig;DB_CLOSE_DELAY=-1");
            dataSource.setUser("sa");
            return dataSource;
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(DataSourceConfig.class)
    static class TestConfig {
        @Bean
        MessagePublisher messagePublisher() {
            return message -> { };
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl reliable-outbox-spring-boot-starter -am test -Dtest=ReliableOutboxAutoConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure — `ReliableOutboxAutoConfiguration` does not exist.

- [ ] **Step 3: Implement the auto-configuration**

`ReliableOutboxAutoConfiguration.java`:

```java
package com.lazycece.outbox.autoconfigure;

import com.lazycece.outbox.client.DefaultMessageClient;
import com.lazycece.outbox.client.MessageClient;
import com.lazycece.outbox.clean.OutboxCleaner;
import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.deadletter.DeadLetterHandler;
import com.lazycece.outbox.deadletter.DeadLetterReplayer;
import com.lazycece.outbox.dispatch.DefaultRetryScheduler;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.dispatch.RetryPolicy;
import com.lazycece.outbox.dispatch.RetryScheduler;
import com.lazycece.outbox.repository.JdbcOutboxRepository;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.scan.MessageScanner;
import com.lazycece.outbox.spi.LoggingMessageListener;
import com.lazycece.outbox.spi.MessageListener;
import com.lazycece.outbox.spi.MessagePublisher;
import com.lazycece.outbox.transaction.SpringTransactionExecutor;
import com.lazycece.outbox.transaction.TransactionExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

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
        return Executors.newScheduledThreadPool(3, namedThreadFactory("reliable-outbox-scheduler-"));
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(PlatformTransactionManager.class)
    public TransactionExecutor transactionExecutor(PlatformTransactionManager transactionManager) {
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
    @ConditionalOnMissingBean(MessagePublisher.class)
    public MessagePublisher missingMessagePublisher() {
        throw new IllegalStateException("reliable-outbox 需要业务方提供 MessagePublisher bean");
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
                                         @Qualifier("reliableOutboxScheduler")
                                         ScheduledExecutorService scheduler) {
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
                                         @Qualifier("reliableOutboxScheduler")
                                         ScheduledExecutorService scheduler,
                                         ReliableOutboxProperties properties) {
        return new MessageScanner(repository, dispatcher, executor, scheduler, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxCleaner outboxCleaner(OutboxRepository repository,
                                       @Qualifier("reliableOutboxScheduler")
                                       ScheduledExecutorService scheduler,
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

- [ ] **Step 4: Register the auto-configuration**

Create `reliable-outbox-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` containing exactly one line:

```text
com.lazycece.outbox.autoconfigure.ReliableOutboxAutoConfiguration
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -pl reliable-outbox-spring-boot-starter -am test -Dtest=ReliableOutboxAutoConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS, 3 tests pass.

- [ ] **Step 6: Commit**

```bash
git add reliable-outbox-spring-boot-starter
git commit -m "feat: add Spring Boot auto-configuration"
```

---

### Task 14: Full verification and example configuration

**Files:**
- Modify: `README.md`
- Test: full suite

**Interfaces:**
- Consumes: everything.
- Produces: green `mvn clean verify`; documented minimal usage.

- [ ] **Step 1: Add a concise README**

Replace the contents of `README.md` with:

```markdown
# reliable-outbox

基于 Transactional Outbox 模式的可靠消息组件。业务数据与 Outbox 记录在本地事务内原子写入，事务提交后异步投递到 MQ，失败由租约抢占 + 数据库扫描兜底重试，最终达到至少一次投递。

- 设计文档：[`reliable-outbox.md`](reliable-outbox.md)
- 投递语义：至少一次；消费端必须幂等
- 不承诺顺序
- 技术基线：Java 17、Spring Boot 4.x
- Maven 坐标：`com.lazycece:reliable-outbox-spring-boot-starter`

## 快速开始

1. 使用 `reliable-outbox-core/src/main/resources/ddl/mysql/t_outbox.sql` 在业务库建表（组件不自动建表）。
2. 引入 starter 并实现 `MessagePublisher`：

```java
@Configuration
public class OutboxConfig {

    @Bean
    public MessagePublisher messagePublisher(KafkaTemplate<String, String> kafka) {
        return message -> {
            // partitionKey 仅用于路由；正常返回必须代表 Broker 已确认
            kafka.send(message.getTopic(), message.getPartitionKey(), message.getPayload());
        };
    }
}
```

3. 发送消息：

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);

    OutboxMessage message = new OutboxMessage();
    message.setBizCode("order");
    message.setTopic("order-events");
    message.setPartitionKey(order.getId());
    message.setPayload(toJson(order));
    messageClient.sendInTransaction(message);
}
```

4. 配置项前缀为 `reliable-outbox`，默认值与含义见设计文档第 8.2 节。
```

- [ ] **Step 2: Run the complete build**

Run: `mvn -q clean verify`
Expected: BUILD SUCCESS; all tests pass in both modules.

- [ ] **Step 3: Confirm no stray build artifacts are staged**

Run: `git status --short`
Expected: only `README.md` and the plan file are modified/untracked; `target/` is ignored.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: add quick start for reliable-outbox"
```

---

## Spec Self-Review

- **Spec coverage:**
  - §3 data model → Task 2 (`OutboxMessage`), Task 4 (DDL + repository).
  - §4 state machine → Task 2 (`OutboxStatus`), Task 9 (transitions).
  - §5.1 entry points / transaction semantics → Task 5, Task 6.
  - §5.2 unified send flow / afterCommit → Task 5, Task 6.
  - §5.3 first trigger + scanner fallback → Task 9, Task 10.
  - §5.4 retry → Task 7, Task 8, Task 9.
  - §5.5 cleanup → Task 11.
  - §5.6 replay → Task 12.
  - §6.1 lease concurrency → Task 4, Task 9.
  - §6.2 at-least-once / markSent failure → Task 9 tests.
  - §6.3 UTC time → Task 4 conversion helpers.
  - §6.4 ordering non-goal → documented; no ordering fields.
  - §6.5 nested transactions → Task 5 tests.
  - §6.6 multi-instance scanning → Task 10 (row-level lease).
  - §6.7 backpressure → Task 1 (bounded pool), Task 10 (capacity check + lease release).
  - §6.8 message size → Task 6 tests.
  - §7 retry policy → Task 7.
  - §8 code design → Tasks 2-13.
  - §9 SPI → Task 3, Task 4, Task 5; `MessageConverter` / `LockManager` intentionally deferred (optional).
  - §10 deployment/monitoring → `MessageListener` SPI in Task 3; no metrics implementation (design only recommends).
  - §11 naming → Global Constraints and package layout.
  - §12 roadmap → first release scope only; no Claim Check / archive tools.
  - §13 constraints → Global Constraints.
- **Placeholder scan:** no TBD/TODO; every task includes concrete code and commands.
- **Type consistency:** `MessageDispatcher` methods match `DefaultMessageClient` / `MessageScanner` / `DeadLetterReplayer`; `OutboxRepository` signatures match all callers and the in-memory test double; `ReliableOutboxProperties` getters match auto-config usage.
- **Task order note:** Task 6 references `MessageDispatcher` which is completed in Task 9. If executing strictly in order, run Task 7 → Task 9 before Task 6, or accept the temporary stub from Task 8 until Task 9 replaces it. The stub is the only forward reference.

