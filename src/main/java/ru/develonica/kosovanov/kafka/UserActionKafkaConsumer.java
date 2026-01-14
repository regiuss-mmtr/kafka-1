package ru.develonica.kosovanov.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import ru.develonica.kosovanov.actor.ActorProperties;
import ru.develonica.kosovanov.actor.EventAddResult;
import ru.develonica.kosovanov.actor.RootActor;
import ru.develonica.kosovanov.actor.UserEvent;
import ru.develonica.kosovanov.model.UserAction;
import ru.develonica.kosovanov.model.UserLimitReach;
import ru.develonica.kosovanov.service.UserLimitReachSendService;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserActionKafkaConsumer {

    public static final int MAX_MESSAGE_PROCESS_RETRY = 600;

    private final Consumer<String, UserAction> consumer;
    private final RootActor rootActor;
    private final UserLimitReachSendService userLimitReachSendService;
    private final TopicProperties topicProperties;
    private final ActorProperties actorProperties;

    private final Thread consumerThread = new Thread(
            this::runConsumer,
            this.getClass().getSimpleName()
    );
    private volatile boolean exitFlag = true;

    @EventListener
    public void onContextRefreshedEvent(ContextRefreshedEvent event) {
        consumerThread.start();
    }

    @EventListener
    public void onContextClosedEvent(ContextClosedEvent gracefullyShutdownStartEvent) {
        exitFlag = false;
        consumer.wakeup();
    }

    private class Listener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            partitions.forEach(rootActor::onPartitionsRevoked);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            long instant = Instant.now().minus(actorProperties.getWindowPeriod().multipliedBy(2)).toEpochMilli();

            Map<TopicPartition, Long> partitionTimeMap = partitions.stream().collect(Collectors.toMap(
                    key -> key,
                    val -> instant
            ));

            consumer.offsetsForTimes(partitionTimeMap);

            partitions.forEach(rootActor::onPartitionsAssigned);
        }

    }

    void runConsumer() {
        try {
            consumer.subscribe(List.of(topicProperties.getConnection()), new Listener());
            while (exitFlag) {
                final ConsumerRecords<String, UserAction> consumerRecords = consumer.poll(Duration.ofMillis(5000));
                boolean messageProcessingNotFinished;
                int failCount = 0;
                do {
                    try {
                        processMessages(consumerRecords);
                        messageProcessingNotFinished = false;
                    } catch (Exception ex) {
                        messageProcessingNotFinished = true;
                        failCount++;
                        if (failCount > MAX_MESSAGE_PROCESS_RETRY) {
                            log.error("Unable to process any message after {} retry", MAX_MESSAGE_PROCESS_RETRY, ex);
                            System.exit(13);
                        } else {
                            log.warn("Unable to process messages", ex);
                            Thread.sleep(1000);
                        }
                    }
                } while (messageProcessingNotFinished);
            }
        } catch (InterruptedException ex) {
            log.error("{} thread execution interrupted", getClass().getSimpleName(), ex);
            exitFlag = false;
        } catch (WakeupException ex) {
            log.info("{} thread finish execution", getClass().getSimpleName(), ex);
        } catch (Exception ex) {
            log.error("kafka internal error when fetching records", ex);
            System.exit(13);

        }
    }

    private void processMessages(ConsumerRecords<String, UserAction> consumerRecords) throws InterruptedException {
        if (consumerRecords.count() > 0) {
            log.info("Records fetched {}", consumerRecords.count());
        } else {
            log.debug("Records fetched {}", consumerRecords.count());
        }

        for (TopicPartition partition : consumerRecords.partitions()) {
            for (ConsumerRecord<String, UserAction> record : consumerRecords.records(partition)) {
                UserAction userAction = record.value();

                long timestamp = record.timestamp();
                Instant eventTime = Instant.ofEpochMilli(timestamp);

                UserEvent event = new UserEvent(userAction.getUserId(), eventTime);

                if (rootActor.addEvent(partition, event) == EventAddResult.ACTIVATION) {
                    userLimitReachSendService.sendUserLimitReach(
                            new UserLimitReach(
                                    userAction.getUserId(),
                                    eventTime
                            )
                    );
                }
            }
        }
    }

}
