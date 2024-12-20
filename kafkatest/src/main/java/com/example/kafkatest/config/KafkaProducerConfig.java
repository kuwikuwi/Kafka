package com.example.kafkatest.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.max-request-size}")
    private int maxRequestSize;

    @Value("${spring.kafka.producer.buffer-memory}")
    private int bufferMemory;


    public Map<String, Object> prodcerConfig(){
        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);  // 15MB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return  props;
    }
    @Bean
    public ProducerFactory<String,String> producerFactory(){
        return new DefaultKafkaProducerFactory<>(prodcerConfig());
    }

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate(ProducerFactory<String,String> producerFactory){
        return new KafkaTemplate<>(producerFactory);
    }
}