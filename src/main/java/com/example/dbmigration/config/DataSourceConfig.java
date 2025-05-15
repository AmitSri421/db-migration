package com.example.dbmigration.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Value("${spring.datasource.source.schema:}")
    private String sourceSchema;

    @Value("${spring.datasource.target.schema:}")
    private String targetSchema;

    @Primary
    @Bean(name = "sourceDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.source")
    public DataSource sourceDataSource() {
        HikariDataSource dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();
        if (sourceSchema != null && !sourceSchema.isEmpty()) {
            dataSource.setSchema(sourceSchema);
        }
        return dataSource;
    }

    @Bean(name = "targetDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.target")
    public DataSource targetDataSource() {
        HikariDataSource dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();
        if (targetSchema != null && !targetSchema.isEmpty()) {
            dataSource.setSchema(targetSchema);
        }
        return dataSource;
    }

    @Primary
    @Bean(name = "sourceJdbcTemplate")
    public JdbcTemplate sourceJdbcTemplate(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "targetJdbcTemplate")
    public JdbcTemplate targetJdbcTemplate(@Qualifier("targetDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
} 