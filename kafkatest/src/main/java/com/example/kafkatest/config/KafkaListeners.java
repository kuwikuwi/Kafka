package com.example.kafkatest.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

@Service
@Slf4j
public class KafkaListeners {

    @Value("${file.save.path}")
    private String savePath;

    private static final Pattern MAPLE_PATTERN = Pattern.compile("MAPLE_RN_\\d{12}_\\d+hr");
    private static final Pattern DFS_ODAM_PATTERN = Pattern.compile("DFS_ODAM_RN1_\\d{12}");
    private static final Pattern DFS_SHRT_PATTERN = Pattern.compile("DFS_SHRT_PCP_\\d{12}_\\d+hr");
    private static final Pattern KLFS_PATTERN = Pattern.compile("KLFS_RN1_\\d{12}_\\d+hr");

    @KafkaListener(topics = "${setting.topics}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> consumerRecord,
                        @Header(KafkaHeaders.OFFSET) Long offset) {
        try {
            String textData = consumerRecord.value();
            String key = consumerRecord.key();

            // Generate filename based on the key
            String fileName = generateFileName(key);

            // Create directory if it doesn't exist and get the file path
            Path path = Paths.get(savePath, fileName);
            Files.createDirectories(path.getParent());

            // Write the data to file
            try (BufferedWriter writer = Files.newBufferedWriter(path)) {
                processAndWriteData(writer, textData, fileName);
            }

            log.info("Text file saved to: {}", path);

        } catch (Exception e) {
            log.error("Error saving text file: {}", e.getMessage(), e);
        }
    }

    private String generateFileName(String key) {
        // If the key (original filename) is provided, use it
        if (key != null && !key.isEmpty()) {
            return key;
        }

        // If somehow no filename is provided, create a default with timestamp
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return "data_" + now.format(formatter) + ".txt";
    }

    private void processAndWriteData(BufferedWriter writer, String textData, String fileName) throws Exception {
        // Split the data into lines
        String[] lines = textData.split("\n");

        // Process each line
        for (String line : lines) {
            // Skip empty lines
            if (line.trim().isEmpty()) {
                continue;
            }

            // Remove any carriage returns and write the line as-is
            String cleanLine = line.trim().replace("\r", "");
            writer.write(cleanLine);
            writer.newLine();
        }
    }
}