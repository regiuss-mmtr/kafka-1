package ru.develonica.kosovanov.actor;

import java.time.Instant;
import java.util.LinkedList;

public class UserActor {

    private final ActorProperties properties;

    private final LinkedList<UserEvent> events = new LinkedList<>();
    private Instant activationTime = Instant.MIN;

    public UserActor(ActorProperties properties) {
        this.properties = properties;
    }

    public EventAddResult addEvent(UserEvent event) {
        events.add(event);

        if (events.size() >= properties.getCountThreshold()) {
            Instant eventTime = event.getEventTime();
            cleanUp(eventTime.minus(properties.getWindowPeriod()));

            if (activationTime == null && events.size() >= properties.getCountThreshold()) {
                activationTime = eventTime;
                return EventAddResult.ACTIVATION;
            }
        }

        return EventAddResult.NONE;
    }

    public CleanUpResult cleanUp(Instant time) {
        events.removeIf(event -> event.getEventTime().isBefore(time));
        if (activationTime != null && activationTime.isBefore(time)) {
            activationTime = null;
        }
        if (events.isEmpty() && activationTime == null) {
            return CleanUpResult.CLEAN;
        } else {
            return CleanUpResult.DIRTY;
        }
    }

    public long queryState() {
        return events.size();
    }

    public enum CleanUpResult {
        CLEAN,
        DIRTY,
    }

}
