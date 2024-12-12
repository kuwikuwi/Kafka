package com.example.kafkatest.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class KafkaProducerInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        log.info("Sending message to topic: {}, partition: {}",
                record.topic(),
                record.partition());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("Failed to send message: {}", exception.getMessage());
            // 여기에 연결 실패 시 추가적인 처리 로직을 구현할 수 있습니다
        }
    }

    @Override
    public void close() {
        log.warn("Producer connection closing");
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}