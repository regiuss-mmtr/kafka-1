package ru.develonica.kosovanov.actor;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class UserEvent {

    private final String userId;

    private final Instant eventTime;

}
