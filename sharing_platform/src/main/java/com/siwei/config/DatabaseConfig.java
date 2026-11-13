package com.siwei.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DatabaseProperties.class)
public class DatabaseConfig {
    @Bean
    @ConfigurationProperties("spring.datasource.hikari")
    HikariDataSource dataSource(DatabaseProperties properties) {
        // Validate required settings before a pool can connect using implicit driver defaults.
        return DataSourceBuilder.create().type(HikariDataSource.class).driverClassName("org.postgresql.Driver")
                .url(properties.getUrl()).username(properties.getUsername()).password(properties.getPassword()).build();
    }
}
