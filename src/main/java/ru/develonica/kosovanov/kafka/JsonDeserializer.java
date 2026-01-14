package ru.develonica.kosovanov.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class JsonDeserializer {

    private final ObjectMapper objectMapper;

    public <T> T deserialize(Class<T> targetClass, byte[] data) {
        if (data.length == 0) {
            return null;
        }

        try {
            return objectMapper.readValue(data, targetClass);
        } catch (IOException e) {
            String message = new String(data, StandardCharsets.UTF_8);
            log.error("Unable to deserialize message {}", message, e);
            return null;
        }
    }

}
