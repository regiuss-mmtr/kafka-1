package ru.develonica.kosovanov.actor;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Getter
@Setter
@Component
@ConfigurationProperties(ActorProperties.PREFIX)
public class ActorProperties {

    public static final String PREFIX = "kafka.actor";

    private int countThreshold;

    private Duration windowPeriod;

    private Duration cleanupPeriod;

}
