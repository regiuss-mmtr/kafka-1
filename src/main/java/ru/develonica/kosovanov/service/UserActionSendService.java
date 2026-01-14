package ru.develonica.kosovanov.service;

import ru.develonica.kosovanov.kafka.TopicProperties;
import ru.develonica.kosovanov.model.UserAction;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class UserActionSendService {

    private final TopicProperties topicProperties;
    private final Producer<String, UserAction> producer;

    public CompletableFuture<RecordMetadata> sendUserAction(UserAction userAction) {
        ProducerRecord<String, UserAction> record = new ProducerRecord<>(
                topicProperties.getConnection(),
                userAction.getUserId(),
                userAction
        );

        CallbackCompletableFuture completableFuture = new CallbackCompletableFuture();
        producer.send(record, completableFuture);
        return completableFuture;
    }

    private static class CallbackCompletableFuture extends CompletableFuture<RecordMetadata> implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                complete(metadata);
            } else {
                completeExceptionally(exception);
            }
        }

    }

}
