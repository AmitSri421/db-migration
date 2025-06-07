package com.example.dbmigration.service.impl;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import com.example.dbmigration.service.ValidationService;
import com.example.dbmigration.util.ValidationUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.io.IOException;
import java.nio.file.*;

@Service
@Slf4j
public class ValidationServiceImpl implements ValidationService {
    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;
    private final String validationHistoryPath;

    public ValidationServiceImpl(JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.validationHistoryPath = "validation_history";
        createValidationHistoryDirectory();
    }

    @Override
    public ValidationResult validate(ValidationRequest request) {
        log.info("Starting validation for table: {} -> {}", request.getSourceTable(), request.getTargetTable());
        LocalDateTime startTime = LocalDateTime.now();
        ValidationResult.ValidationResultBuilder resultBuilder = ValidationResult.builder()
                .sourceTable(request.getSourceTable())
                .targetTable(request.getTargetTable())
                .validationType(request.getValidationType())
                .validationStartTime(startTime);

        try {
            switch (request.getValidationType()) {
                case TABLE:
                    validateTable(request, resultBuilder);
                    break;
                case PARTITION:
                    validatePartition(request, resultBuilder);
                    break;
                case BOTH:
                    validateTable(request, resultBuilder);
                    validatePartition(request, resultBuilder);
                    break;
            }
            resultBuilder.success(true);
        } catch (Exception e) {
            log.error("Validation failed for table: {} -> {}", request.getSourceTable(), request.getTargetTable(), e);
            resultBuilder.success(false)
                    .errorMessage(e.getMessage());
        }

        LocalDateTime endTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, endTime);
        resultBuilder.validationEndTime(endTime)
                .validationDuration(duration.toString());

        ValidationResult result = resultBuilder.build();
        saveValidationResult(result);
        return result;
    }

    @Override
    public Map<String, ValidationResult> validateBatch(List<ValidationRequest> requests) {
        Map<String, ValidationResult> results = new HashMap<>();
        for (ValidationRequest request : requests) {
            ValidationResult result = validate(request);
            String key = request.getValidationType() == ValidationRequest.ValidationType.PARTITION ?
                    request.getSourceTable() + "." + request.getPartitionKey() :
                    request.getSourceTable();
            results.put(key, result);
        }
        return results;
    }

    @Override
    public String generateReport(List<ValidationResult> results) {
        String reportPath = validationHistoryPath + "/validation_report_" +
                LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".txt";
        try {
            ValidationUtil.writeValidationReport(results, reportPath);
            return reportPath;
        } catch (IOException e) {
            log.error("Failed to generate validation report", e);
            throw new RuntimeException("Failed to generate validation report", e);
        }
    }

    @Override
    public List<ValidationResult> getValidationHistory(String tableName, String startDate, String endDate) {
        // TODO: Implement validation history retrieval from database or file system
        return new ArrayList<>();
    }

    private void validateTable(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        if (request.isValidateRowCount()) {
            long sourceCount = ValidationUtil.getRowCount(sourceJdbcTemplate, request.getSourceTable(), request.getWhereClause());
            long targetCount = ValidationUtil.getRowCount(targetJdbcTemplate, request.getTargetTable(), request.getWhereClause());
            resultBuilder.sourceRowCount(sourceCount)
                    .targetRowCount(targetCount)
                    .rowCountMatch(sourceCount == targetCount);
        }

        if (request.isValidateData() && request.getColumns() != null && !request.getColumns().isEmpty()) {
            Map<String, Long> mismatchCounts = ValidationUtil.compareColumnData(
                    sourceJdbcTemplate,
                    targetJdbcTemplate,
                    request.getSourceTable(),
                    request.getTargetTable(),
                    request.getColumns(),
                    request.getWhereClause()
            );
            resultBuilder.columnMismatchCounts(mismatchCounts)
                    .mismatchedColumns(new ArrayList<>(mismatchCounts.keySet()));
        }
    }

    private void validatePartition(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        if (request.isValidatePartitions()) {
            List<String> sourcePartitions = ValidationUtil.getPartitions(sourceJdbcTemplate, request.getSourceTable());
            List<String> targetPartitions = ValidationUtil.getPartitions(targetJdbcTemplate, request.getTargetTable());

            if (request.getPartitions() != null && !request.getPartitions().isEmpty()) {
                sourcePartitions.retainAll(request.getPartitions());
                targetPartitions.retainAll(request.getPartitions());
            }

            List<String> missingPartitions = new ArrayList<>(sourcePartitions);
            missingPartitions.removeAll(targetPartitions);

            List<String> extraPartitions = new ArrayList<>(targetPartitions);
            extraPartitions.removeAll(sourcePartitions);

            resultBuilder.missingPartitions(missingPartitions)
                    .extraPartitions(extraPartitions);
        }
    }

    private void createValidationHistoryDirectory() {
        try {
            Files.createDirectories(Paths.get(validationHistoryPath));
        } catch (IOException e) {
            log.error("Failed to create validation history directory", e);
            throw new RuntimeException("Failed to create validation history directory", e);
        }
    }

    private void saveValidationResult(ValidationResult result) {
        String resultPath = validationHistoryPath + "/" + result.getSourceTable() + "_" +
                LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".json";
        try {
            // TODO: Implement saving validation result to file or database
        } catch (Exception e) {
            log.error("Failed to save validation result", e);
        }
    }
} 