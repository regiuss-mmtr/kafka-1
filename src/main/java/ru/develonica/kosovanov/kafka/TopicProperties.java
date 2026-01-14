package ru.develonica.kosovanov.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(TopicProperties.PREFIX)
public class TopicProperties {

    public static final String PREFIX = "kafka.topic";

    private String connection;

    private String limit;

}
