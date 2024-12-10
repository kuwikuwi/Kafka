package com.example.kafkatest.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class    TaskExecutorConfig {

    @Bean
    public TaskExecutor executor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(3);        // 기본 실행 대기하는 Thread 수
        executor.setMaxPoolSize(5);         // 동시 동작하는 최대 Thread 수
        executor.setQueueCapacity(10);      // MaxPoolSize 초과 요청에서 Thread 생성 요청시 해당 요청을 Queue에 저장하는데 이때 최대 수용 가능한 Queue의 수
        executor.setThreadNamePrefix("kafka-executor-"); // Thread 생성시 prefix로 사용할 이름
        executor.initialize();
        return executor;
    }
}