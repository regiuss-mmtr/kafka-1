package ru.develonica.kosovanov.actor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class PartitionActor {

    private final ActorProperties properties;

    private final HashMap<String, UserActor> subActorMap = new HashMap<>();
    private Instant nextCleanup = Instant.MIN;

    public EventAddResult addEvent(UserEvent event) {
        if (nextCleanup.isBefore(event.getEventTime())) {
            cleanUp(event.getEventTime());
        }

        return subActorMap
                .computeIfAbsent(event.getUserId(), s -> new UserActor(properties))
                .addEvent(event);
    }

    public void cleanUp(Instant time) {
        Instant lowThreshold = time.minus(properties.getWindowPeriod());
        subActorMap.values().removeIf(actor -> actor.cleanUp(lowThreshold) == UserActor.CleanUpResult.CLEAN);
        nextCleanup = time.plus(properties.getCleanupPeriod());
    }

    public Map<String, Long> queryState() {
        return subActorMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().queryState()
                ));
    }

}
