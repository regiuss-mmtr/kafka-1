package ru.develonica.kosovanov.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserAction {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("action_type")
    private String actionType;

}
