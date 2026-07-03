package com.example.replicator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DdlSanitizerTest {
    @Test
    void relaxedCreateTableKeepsColumnsAndPrimaryKeyOnly() {
        String ddl = """
                CREATE TABLE `child` (
                  `id` int NOT NULL AUTO_INCREMENT,
                  `user_id` int NOT NULL,
                  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                  `active` tinyint(1) NOT NULL DEFAULT '1',
                  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  `full_name` varchar(500) GENERATED ALWAYS AS (concat(`name`, _utf8mb4'')) STORED,
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `uk_user_id` (`user_id`),
                  KEY `idx_name` (`name`),
                  FULLTEXT KEY `ft_name` (`name`),
                  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`),
                  CONSTRAINT `chk_id` CHECK ((`id` > 0))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """;

        String relaxed = DdlSanitizer.prepare(ddl, "source_db", "sink_db", false).orElseThrow();

        assertThat(relaxed).contains("`id` int");
        assertThat(relaxed).contains("`user_id` int");
        assertThat(relaxed).contains("`name` longtext");
        assertThat(relaxed).contains("`active` tinyint(1)");
        assertThat(relaxed).contains("`updated_at` timestamp");
        assertThat(relaxed).doesNotContain("NOT NULL");
        assertThat(relaxed).doesNotContain("AUTO_INCREMENT");
        assertThat(relaxed).doesNotContain("DEFAULT");
        assertThat(relaxed).doesNotContain("ON UPDATE");
        assertThat(relaxed).doesNotContain("CHARACTER SET");
        assertThat(relaxed).doesNotContain("COLLATE");
        assertThat(relaxed).doesNotContain("full_name");
        assertThat(relaxed).contains("PRIMARY KEY (`id`)");
        assertThat(relaxed).doesNotContain("UNIQUE KEY");
        assertThat(relaxed).doesNotContain("KEY `idx_name`");
        assertThat(relaxed).doesNotContain("FULLTEXT KEY");
        assertThat(relaxed).doesNotContain("FOREIGN KEY");
        assertThat(relaxed).doesNotContain("CHECK");
    }

    @Test
    void relaxedModeSkipsIndexOnlyDdl() {
        assertThat(DdlSanitizer.prepare("CREATE INDEX idx_name ON user_model(name)", "source_db", "sink_db", false))
                .isEmpty();
        assertThat(DdlSanitizer.prepare("ALTER TABLE user_model ADD KEY idx_name (name)", "source_db", "sink_db", false))
                .isEmpty();
    }

    @Test
    void strictModeKeepsDdlUnchangedExceptSchemaMapping() {
        String ddl = "CREATE TABLE `source_db`.`user_model` (`id` int PRIMARY KEY, UNIQUE KEY `uk_id` (`id`))";

        String strict = DdlSanitizer.prepare(ddl, "source_db", "sink_db", true).orElseThrow();

        assertThat(strict).contains("`sink_db`.`user_model`");
        assertThat(strict).contains("UNIQUE KEY");
    }
}
