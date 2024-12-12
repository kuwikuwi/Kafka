package com.example.kafkatest.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@Slf4j
public class KafkaListeners {

    @Value("${file.save.path}")
    private String savePath;

    @KafkaListener(topics = "${setting.topics}",groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> consumerRecord,
                        @Header(KafkaHeaders.OFFSET) Long offset) {
        try {
            String csvData = consumerRecord.value();
            String fileName = String.format("data_%s.xlsx", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
            // 엑셀 만들기
            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Data");

            // 엑셀 행 생성
            String[] rows = csvData.split("\n");
            for (int i = 0; i < rows.length; i++) {
                Row row = sheet.createRow(i);
                String[] cells = rows[i].split(",");
                for (int j = 0; j < cells.length; j++) {
                    Cell cell = row.createCell(j);
                    cell.setCellValue(cells[j].trim());
                }
            }

            // 엑셀 저장
            Path path = Paths.get(savePath, fileName);
            Files.createDirectories(path.getParent());
            try (FileOutputStream fos = new FileOutputStream(path.toFile())) {
                workbook.write(fos);
            }
            workbook.close();

            log.info("Excel file saved to: {}", path);

        } catch (Exception e) {
            log.error("Error saving Excel file: {}", e.getMessage(), e);
        }
    }
}