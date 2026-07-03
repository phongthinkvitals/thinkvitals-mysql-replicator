package com.example.replicator;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.Container;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MysqlBinlogReplicatorIntegrationTest {
    @Container
    static MySQLContainer<?> source = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("thinkvitals")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("source-init.sql")
            .withCommand("--server-id=301", "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL");

    @Container
    static MySQLContainer<?> sink = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("thinkvitals_bcp")
            .withUsername("test")
            .withPassword("test");

    @Autowired
    ReplicationService replicationService;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("source.url", source::getJdbcUrl);
        registry.add("source.username", source::getUsername);
        registry.add("source.password", source::getPassword);
        registry.add("sink.url", sink::getJdbcUrl);
        registry.add("sink.username", sink::getUsername);
        registry.add("sink.password", sink::getPassword);
    }

    @Test
    void snapshotsAndReplicatesDmlAndDdl() throws Exception {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            try (Connection sinkConnection = DriverManager.getConnection(sink.getJdbcUrl(), sink.getUsername(), sink.getPassword())) {
                var rs = sinkConnection.createStatement().executeQuery("SELECT name FROM user_model WHERE id = 1");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo("alice");
            }
        });

        try (Connection sourceConnection = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
            sourceConnection.createStatement().execute("INSERT INTO user_model(id, name) VALUES (2, 'bob')");
            sourceConnection.createStatement().execute("UPDATE user_model SET name = 'alice2' WHERE id = 1");
            sourceConnection.createStatement().execute("DELETE FROM user_model WHERE id = 2");
            sourceConnection.createStatement().execute("ALTER TABLE user_model ADD COLUMN age INT NULL");
            sourceConnection.createStatement().execute("UPDATE user_model SET age = 42 WHERE id = 1");
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            try (Connection sinkConnection = DriverManager.getConnection(sink.getJdbcUrl(), sink.getUsername(), sink.getPassword())) {
                var rs = sinkConnection.createStatement().executeQuery("SELECT name, age FROM user_model WHERE id = 1");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("name")).isEqualTo("alice2");
                assertThat(rs.getInt("age")).isEqualTo(42);
                var deleted = sinkConnection.createStatement().executeQuery("SELECT COUNT(*) FROM user_model WHERE id = 2");
                deleted.next();
                assertThat(deleted.getInt(1)).isZero();
            }
        });

        try (Connection sinkConnection = DriverManager.getConnection(sink.getJdbcUrl(), sink.getUsername(), sink.getPassword())) {
            sinkConnection.createStatement().execute("DROP TABLE user_model");
        }
        try (Connection sourceConnection = DriverManager.getConnection(source.getJdbcUrl(), source.getUsername(), source.getPassword())) {
            sourceConnection.createStatement().execute("UPDATE user_model SET name = 'alice3', age = 43 WHERE id = 1");
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            try (Connection sinkConnection = DriverManager.getConnection(sink.getJdbcUrl(), sink.getUsername(), sink.getPassword())) {
                var rs = sinkConnection.createStatement().executeQuery("SELECT name, age FROM user_model WHERE id = 1");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("name")).isEqualTo("alice3");
                assertThat(rs.getInt("age")).isEqualTo(43);
            }
        });
    }
}
