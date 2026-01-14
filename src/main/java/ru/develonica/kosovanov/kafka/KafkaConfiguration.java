package ru.develonica.kosovanov.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.develonica.kosovanov.model.UserAction;
import ru.develonica.kosovanov.model.UserLimitReach;

import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean
    @ConfigurationProperties("kafka.connection.consumer")
    public Properties kafkaConnectionConsumerProperties() {
        return new Properties();
    }

    @Bean
    @ConfigurationProperties("kafka.connection.producer")
    public Properties kafkaConnectionProducerProperties() {
        return new Properties();
    }

    @Bean
    @ConfigurationProperties("kafka.limit.producer")
    public Properties kafkaLimitProducerProperties() {
        return new Properties();
    }

    @Bean(destroyMethod = "close")
    public Consumer<String, UserAction> createConsumer(Properties kafkaConnectionConsumerProperties, JsonDeserializer deserializer) {
        return new KafkaConsumer<>(
                kafkaConnectionConsumerProperties,
                null,
                (topic, data) -> deserializer.deserialize(UserAction.class, data)
        );
    }

    @Bean(destroyMethod = "close")
    public KafkaProducer<String, UserAction> createUserActionProducer(Properties kafkaConnectionProducerProperties, JsonSerializer serializer) {
        return new KafkaProducer<>(kafkaConnectionProducerProperties, null, serializer::serialize);
    }

    @Bean(destroyMethod = "close")
    public KafkaProducer<String, UserLimitReach> createUserLimitReachProducer(Properties kafkaLimitProducerProperties, JsonSerializer serializer) {
        return new KafkaProducer<>(kafkaLimitProducerProperties, null, serializer::serialize);
    }

}

