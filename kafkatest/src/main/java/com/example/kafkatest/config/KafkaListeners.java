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
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@Slf4j
public class KafkaListeners {
    // local에 원본 파일 저장
    @Value("${file.save.path}")
    private String savePath;

    // 데이터베이스 연결 정보
    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;
    
    // 데이터 이름들로 하드 코딩
//    private static final Pattern MAPLE_PATTERN = Pattern.compile("MAPLE_RN_\\d{12}_\\d+hr");
//    private static final Pattern DFS_ODAM_PATTERN = Pattern.compile("DFS_ODAM_RN1_\\d{12}");
//    private static final Pattern DFS_SHRT_PATTERN = Pattern.compile("DFS_SHRT_PCP_\\d{12}_\\d+hr");
//    private static final Pattern KLFS_PATTERN = Pattern.compile("KLFS_RN1_\\d{12}_\\d+hr");

    // 데이터베이스 연결을 얻는 메소드
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
    }

    @KafkaListener(topics = "${setting.topics}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> consumerRecord,
                        @Header(KafkaHeaders.OFFSET) Long offset) {
        try {
            String textData = consumerRecord.value();
            String key = consumerRecord.key();

            // 파일 시스템에 저장
            String fileName = generateFileName(key);
            Path path = Paths.get(savePath, fileName);
            Files.createDirectories(path.getParent());
            try (BufferedWriter writer = Files.newBufferedWriter(path)) {
                processAndWriteData(writer, textData, fileName);
            }

            // DB에 저장
            saveToDatabase(fileName, textData);
            log.info("Text file saved to: {}", path);

        } catch (Exception e) {
            log.error("Error saving text file: {}", e.getMessage(), e);
        }
    }
    
    // DB 저장 처리 메서드
    private void saveToDatabase(String fileName, String textData) {
        // 테이블 이름으로 사용할 수 있게 파일 이름 정제
        String tableName = fileName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();

        try (Connection conn = getConnection()) {
            // 자동 커밋 비활성화 (배치 처리를 위해)
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                // 테이블 있을 경우 지우고 새로 생성
                String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", tableName);
                stmt.execute(dropTableSQL);

                // 테이블 새로 생성
                String createTableSQL = String.format(
                        "CREATE TABLE %s (" +
                                "fid INTEGER, " +
                                "value DOUBLE PRECISION" +
                                ")", tableName);

                stmt.execute(createTableSQL);
            }

            // 데이터 삽입 준비
            String insertSQL = String.format("INSERT INTO %s (fid, value) VALUES (?, ?)", tableName);

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                // 데이터 파싱 및 삽입
                String[] lines = textData.split("\n");
                int batchSize = 0;

                for (String line : lines) {
                    if (line.trim().isEmpty()) continue;

                    String[] values = line.split(",");
                    if (values.length == 2) {
                        try {
                            pstmt.setInt(1, Integer.parseInt(values[0].trim()));
                            pstmt.setDouble(2, Double.parseDouble(values[1].trim()));
                            pstmt.addBatch();

                            // 1000개마다 배치 실행
                            if (++batchSize % 1000 == 0) {
                                pstmt.executeBatch();
                                conn.commit();
                            }
                        } catch (NumberFormatException e) {
                            log.warn("Skipping invalid line: {}", line);
                        }
                    }
                }

                // 남은 배치 실행
                if (batchSize % 1000 != 0) {
                    pstmt.executeBatch();
                    conn.commit();
                }
            }

            log.info("Successfully saved data to database table: {}", tableName);

        } catch (SQLException e) {
            log.error("Database error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to save to database", e);
        }
    }

    private String generateFileName(String key) {
        // 파일 이름 존재할 경우 그대로 사용
        if (key != null && !key.isEmpty()) {
            return key;
        }

        // 파일 이름이 존재안할 경우, 시간 넣어서 생성
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return "data_" + now.format(formatter) + ".txt";
    }

    private void processAndWriteData(BufferedWriter writer, String textData, String fileName) throws Exception {
        // 데이터 분리
        String[] lines = textData.split("\n");

        // 각 행 처리
        for (String line : lines) {
            // 빈칸일 경우 건너뛰기
            if (line.trim().isEmpty()) {
                continue;
            }

            // 줄변경 있을경우 없애기
            String cleanLine = line.trim().replace("\r", "");
            writer.write(cleanLine);
            writer.newLine();
        }
    }
}