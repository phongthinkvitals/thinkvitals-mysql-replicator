package com.example.replicator.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {
    @Bean
    DataSource sourceDataSource(ReplicatorProperties properties) {
        return dataSource(properties.getSource());
    }

    @Bean
    DataSource sinkDataSource(ReplicatorProperties properties) {
        return dataSource(properties.getSink());
    }

    @Bean
    JdbcTemplate sourceJdbcTemplate(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    JdbcTemplate sinkJdbcTemplate(@Qualifier("sinkDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    private HikariDataSource dataSource(ReplicatorProperties.Db db) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(db.getUrl());
        ds.setUsername(db.getUsername());
        ds.setPassword(db.getPassword());
        ds.setMaximumPoolSize(5);
        return ds;
    }
}
