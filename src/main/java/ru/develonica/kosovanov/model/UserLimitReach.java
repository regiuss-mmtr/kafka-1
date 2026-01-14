package ru.develonica.kosovanov.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class UserLimitReach {

    private final String userId;

    private final Instant reachTime;

}
