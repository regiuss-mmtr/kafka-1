package ru.develonica.kosovanov.actor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class RootActor {

    private final ActorProperties properties;
    private final HashMap<TopicPartition, PartitionActor> partitionMap = new HashMap<>();

    public void onPartitionsAssigned(TopicPartition partition) {
        partitionMap.put(partition, new PartitionActor(properties));
    }

    public void onPartitionsRevoked(TopicPartition partition) {
        partitionMap.remove(partition);
    }

    public EventAddResult addEvent(TopicPartition partition, UserEvent event) {
        PartitionActor actor = partitionMap.get(partition);
        if (actor == null) {
            log.error("Event on partition {}, but it not assigned", partition);
            return EventAddResult.NONE;
        } else {
            return actor.addEvent(event);
        }
    }

    public Map<String, Long> queryState() {
        return partitionMap.values().stream()
                .flatMap(partitionActor -> partitionActor.queryState().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
