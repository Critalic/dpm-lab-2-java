package com.kpi.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

@ConfigurationPropertiesScan
@SpringBootApplication
public class ConsumerApplication {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(ConsumerApplication.class, args);
        var message = "\nTime now " + Instant.now().toString();
        Path path = Paths.get("/Users/vitaliikryvonosiuk/IdeaProjects/dpm-lab-2/consumer/src/main/resources/output.txt");
        Files.write(path, message.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

}
