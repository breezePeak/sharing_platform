package com.siwei;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@org.springframework.scheduling.annotation.EnableScheduling
public class App {
    @org.springframework.context.annotation.Bean
    java.time.Clock clock() { return java.time.Clock.systemUTC(); }

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
