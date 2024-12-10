package com.example.kafkatest.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaMessageService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${test.project.topic}")
    private String topic;

    public void sendMessage(String message) {
        try {
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Message sent successfully: {}, partition: {}, offset: {}",
                            message,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to send message: {}", ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Error while sending message: {}", e.getMessage());
            throw new RuntimeException("Failed to send message", e);
        }
    }

    public void sendMessageWithKey(String key, String message) {
        try {
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, key, message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Message sent successfully - Key: {}, Message: {}, Partition: {}, Offset: {}",
                            key,
                            message,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to send message with key {}: {}", key, ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Error while sending message with key {}: {}", key, e.getMessage());
            throw new RuntimeException("Failed to send message", e);
        }
    }
}