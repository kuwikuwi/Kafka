package com.example.kafkatest.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
@Slf4j
public class KafkaController {

    private final KafkaMessageService kafkaMessageService;

    // properties에서 파라미터 이름을 가져옵니다
    @Value("${producer.file.param}")
    private String fileParam;
    @PostMapping(value = "/send", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> sendMessage(@RequestParam Map<String, MultipartFile> files) {
        try {
            MultipartFile file = files.get(fileParam);
            if (file != null && !file.isEmpty()) {
                // 파일 원본 이름 가져오기
                String originalFilename = file.getOriginalFilename();

                // Read the file content
                String content = new BufferedReader(
                        new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));

                // Send the message with the original filename as the key
                kafkaMessageService.sendMessageWithKey(originalFilename, content);

                return ResponseEntity.ok("File sent successfully with name: " + originalFilename);
            }
            return ResponseEntity.badRequest().body("File content is required");
        } catch (Exception e) {
            log.error("Failed to send message: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send message: " + e.getMessage());
        }
    }

    @PostMapping(value = "/send/{key}", consumes = {MediaType.TEXT_PLAIN_VALUE,
            MediaType.APPLICATION_JSON_VALUE,
            MediaType.MULTIPART_FORM_DATA_VALUE})
    public ResponseEntity<String> sendMessageWithKey(
            @PathVariable String key,
            @RequestParam(value = "#{${producer.file.param}}", required = false) MultipartFile file,
            @RequestBody(required = false) String message) {
        try {
            String content;

            // Handle file upload if present
            if (file != null && !file.isEmpty()) {
                // Read content from the uploaded file
                content = new BufferedReader(
                        new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
            } else if (message != null && !message.isEmpty()) {
                // Use the direct message content
                content = message;
            } else {
                return ResponseEntity.badRequest()
                        .body("Either file or message content is required");
            }

            kafkaMessageService.sendMessageWithKey(key, content);
            return ResponseEntity.ok("Message sent successfully with key: " + key);
        } catch (Exception e) {
            log.error("Failed to send message with key {}: {}", key, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send message: " + e.getMessage());
        }
    }
}