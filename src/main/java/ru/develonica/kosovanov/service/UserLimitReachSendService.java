package ru.develonica.kosovanov.service;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.develonica.kosovanov.kafka.TopicProperties;
import ru.develonica.kosovanov.model.UserLimitReach;

import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserLimitReachSendService {

    private final Producer<String, UserLimitReach> producer;
    private final TopicProperties topicProperties;

    public void sendUserLimitReach(UserLimitReach limitReach) {
        log.info("Limit reach for user {} at {}", limitReach.getUserId(), limitReach.getReachTime());
        ProducerRecord<String, UserLimitReach> record =
                new ProducerRecord<>(
                        topicProperties.getLimit(),
                        limitReach.getUserId(),
                        limitReach
                );

        try {
            producer.send(record).get();
        } catch (InterruptedException ex) {
            log.warn("Await interrupted", ex);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException)ex.getCause();
            } else {
                throw new RuntimeException("Unable to send message in kafka", ex.getCause());
            }
        }
    }
}