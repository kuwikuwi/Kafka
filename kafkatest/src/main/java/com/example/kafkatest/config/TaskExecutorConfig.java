package com.example.kafkatest.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class TaskExecutorConfig {

    @Value("${kafka.thread.core-pool-size}")
    private int corePoolSize;

    @Value("${kafka.thread.max-pool-size}")
    private int maxPoolSize;

    @Value("${kafka.thread.queue-capacity}")
    private int queueCapacity;

    @Value("${kafka.thread.prefix}")
    private String threadNamePrefix;

    @Bean
    public TaskExecutor executor() {
        // ThreadPoolTaskExecutor는 Spring이 제공하는 ThreadPool 관리 클래스입니다
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // properties 파일에서 읽어온 값들을 설정합니다
        executor.setCorePoolSize(corePoolSize);        // 기본적으로 실행 대기하는 Thread 수
        executor.setMaxPoolSize(maxPoolSize);          // 동시 동작하는 최대 Thread 수
        executor.setQueueCapacity(queueCapacity);      // Thread Pool이 가득 찼을 때 대기할 수 있는 최대 요청 수
        executor.setThreadNamePrefix(threadNamePrefix); // Thread 이름의 접두사 설정

        // ThreadPool을 초기화합니다
        executor.initialize();

        return executor;
    }
}