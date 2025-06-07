package com.example.dbmigration.service.impl;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import com.example.dbmigration.model.ValidationType;
import com.example.dbmigration.service.ValidationService;
import com.example.dbmigration.util.MetadataQueries;
import com.example.dbmigration.util.ValidationUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
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
        
        ValidationResult.ValidationResultBuilder resultBuilder = ValidationResult.builder()
                .sourceTable(request.getSourceTable())
                .targetTable(request.getTargetTable())
                .validationType(ValidationType.valueOf(request.getValidationType().name()))
                .startTime(new Date());

        try {
            // If partitions are specified, validate only those partitions
            if (request.getValidationType() == ValidationRequest.ValidationType.PARTITION && 
                request.getPartitions() != null && !request.getPartitions().isEmpty()) {
                validatePartitions(request, resultBuilder);
            } else {
                // Validate row counts
                if (request.isValidateRowCount()) {
                    validateRowCounts(request, resultBuilder);
                }

                // Validate indexes
                if (request.isValidateIndexes()) {
                    validateIndexes(request, resultBuilder);
                }

                // Validate constraints
                if (request.isValidateConstraints()) {
                    validatePrimaryKeys(request, resultBuilder);
                    validateForeignKeys(request, resultBuilder);
                    validateUniqueKeys(request, resultBuilder);
                }

                // Validate null/not-null constraints
                if (request.isValidateNullConstraints()) {
                    validateNullConstraints(request, resultBuilder);
                }

                // Validate data types and default values
                if (request.isValidateDataTypes()) {
                    validateDataTypes(request, resultBuilder);
                }

                // Validate partition strategy
                if (request.isValidatePartitionStrategy()) {
                    validatePartitionStrategy(request, resultBuilder);
                }

                // Validate column order
                if (request.isValidateColumnOrder()) {
                    validateColumnOrder(request, resultBuilder);
                }
            }

            resultBuilder.success(true);
        } catch (Exception e) {
            log.error("Validation failed for table: {} -> {}", request.getSourceTable(), request.getTargetTable(), e);
            resultBuilder.success(false)
                    .errorMessage(e.getMessage());
        }

        resultBuilder.endTime(new Date());
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
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String reportPath = validationHistoryPath + "/validation_report_" + timestamp + ".txt";
        
        try {
            ValidationUtil.writeValidationReport(results, reportPath);
            return reportPath;
        } catch (IOException e) {
            log.error("Failed to generate validation report", e);
            throw new RuntimeException("Failed to generate validation report", e);
        }
    }

    private void validateRowCounts(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        Long sourceCount = sourceJdbcTemplate.queryForObject(
            String.format(MetadataQueries.GET_ROW_COUNT, request.getSourceTable()),
            Long.class
        );
        Long targetCount = targetJdbcTemplate.queryForObject(
            String.format(MetadataQueries.GET_ROW_COUNT, request.getTargetTable()),
            Long.class
        );

        resultBuilder.sourceRowCount(sourceCount)
                .targetRowCount(targetCount)
                .rowCountMatch(Objects.equals(sourceCount, targetCount));
    }

    private void validateIndexes(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<Map<String, Object>> sourceIndexes = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_INDEXES,
            request.getSourceTable()
        );
        List<Map<String, Object>> targetIndexes = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_INDEXES,
            request.getTargetTable()
        );

        Set<String> sourceIndexSet = sourceIndexes.stream()
            .map(index -> index.get("INDEX_NAME").toString())
            .collect(Collectors.toSet());
        Set<String> targetIndexSet = targetIndexes.stream()
            .map(index -> index.get("INDEX_NAME").toString())
            .collect(Collectors.toSet());

        resultBuilder.missingIndexes(new ArrayList<>(sourceIndexSet.stream()
            .filter(index -> !targetIndexSet.contains(index))
            .collect(Collectors.toSet())))
            .extraIndexes(new ArrayList<>(targetIndexSet.stream()
                .filter(index -> !sourceIndexSet.contains(index))
                .collect(Collectors.toSet())));
    }

    private void validatePrimaryKeys(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<String> sourcePKs = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_PRIMARY_KEYS,
            String.class,
            request.getSourceTable(),
            request.getSourceTable()
        );
        List<String> targetPKs = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_PRIMARY_KEYS,
            String.class,
            request.getTargetTable(),
            request.getTargetTable()
        );

        resultBuilder.missingPrimaryKeys(new ArrayList<>(sourcePKs.stream()
            .filter(pk -> !targetPKs.contains(pk))
            .collect(Collectors.toSet())))
            .extraPrimaryKeys(new ArrayList<>(targetPKs.stream()
                .filter(pk -> !sourcePKs.contains(pk))
                .collect(Collectors.toSet())));
    }

    private void validateForeignKeys(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<Map<String, Object>> sourceFKs = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_FOREIGN_KEYS,
            request.getSourceTable()
        );
        List<Map<String, Object>> targetFKs = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_FOREIGN_KEYS,
            request.getTargetTable()
        );

        Set<String> sourceFKSet = sourceFKs.stream()
            .map(fk -> fk.get("CONSTRAINT_NAME").toString())
            .collect(Collectors.toSet());
        Set<String> targetFKSet = targetFKs.stream()
            .map(fk -> fk.get("CONSTRAINT_NAME").toString())
            .collect(Collectors.toSet());

        resultBuilder.missingForeignKeys(new ArrayList<>(sourceFKSet.stream()
            .filter(fk -> !targetFKSet.contains(fk))
            .collect(Collectors.toSet())))
            .extraForeignKeys(new ArrayList<>(targetFKSet.stream()
                .filter(fk -> !sourceFKSet.contains(fk))
                .collect(Collectors.toSet())));
    }

    private void validateUniqueKeys(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<String> sourceUKs = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_UNIQUE_KEYS,
            String.class,
            request.getSourceTable(),
            request.getSourceTable()
        );
        List<String> targetUKs = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_UNIQUE_KEYS,
            String.class,
            request.getTargetTable(),
            request.getTargetTable()
        );

        resultBuilder.missingUniqueKeys(new ArrayList<>(sourceUKs.stream()
            .filter(uk -> !targetUKs.contains(uk))
            .collect(Collectors.toSet())))
            .extraUniqueKeys(new ArrayList<>(targetUKs.stream()
                .filter(uk -> !sourceUKs.contains(uk))
                .collect(Collectors.toSet())));
    }

    private void validateNullConstraints(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<Map<String, Object>> sourceColumns = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_COLUMNS,
            request.getSourceTable()
        );
        List<Map<String, Object>> targetColumns = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_COLUMNS,
            request.getTargetTable()
        );

        Map<String, String> sourceNullMap = sourceColumns.stream()
            .collect(Collectors.toMap(
                col -> col.get("COLUMN_NAME").toString(),
                col -> col.get("NULLABLE").toString()
            ));
        Map<String, String> targetNullMap = targetColumns.stream()
            .collect(Collectors.toMap(
                col -> col.get("COLUMN_NAME").toString(),
                col -> col.get("NULLABLE").toString()
            ));

        List<String> mismatches = new ArrayList<>();
        sourceNullMap.forEach((col, nullable) -> {
            if (!targetNullMap.containsKey(col) || !targetNullMap.get(col).equals(nullable)) {
                mismatches.add(col);
            }
        });

        resultBuilder.nullConstraintMismatches(mismatches);
    }

    private void validateDataTypes(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<Map<String, Object>> sourceColumns = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_COLUMNS,
            request.getSourceTable()
        );
        List<Map<String, Object>> targetColumns = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_COLUMNS,
            request.getTargetTable()
        );

        Map<String, String> sourceTypeMap = sourceColumns.stream()
            .collect(Collectors.toMap(
                col -> col.get("COLUMN_NAME").toString(),
                col -> String.format("%s(%s,%s)",
                    col.get("DATA_TYPE"),
                    col.get("DATA_PRECISION"),
                    col.get("DATA_SCALE"))
            ));
        Map<String, String> targetTypeMap = targetColumns.stream()
            .collect(Collectors.toMap(
                col -> col.get("COLUMN_NAME").toString(),
                col -> String.format("%s(%s,%s)",
                    col.get("DATA_TYPE"),
                    col.get("DATA_PRECISION"),
                    col.get("DATA_SCALE"))
            ));

        List<String> mismatches = new ArrayList<>();
        sourceTypeMap.forEach((col, type) -> {
            if (!targetTypeMap.containsKey(col) || !targetTypeMap.get(col).equals(type)) {
                mismatches.add(col + ": " + type + " vs " + targetTypeMap.get(col));
            }
        });

        resultBuilder.dataTypeMismatches(mismatches);
    }

    private void validatePartitionStrategy(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        Map<String, Object> sourcePartitionType = sourceJdbcTemplate.queryForMap(
            MetadataQueries.GET_PARTITION_TYPE,
            request.getSourceTable()
        );
        Map<String, Object> targetPartitionType = targetJdbcTemplate.queryForMap(
            MetadataQueries.GET_PARTITION_TYPE,
            request.getTargetTable()
        );

        String sourceType = sourcePartitionType.get("PARTITIONING_TYPE").toString();
        String targetType = targetPartitionType.get("PARTITIONING_TYPE").toString();
        String sourceSubType = sourcePartitionType.get("SUBPARTITIONING_TYPE") != null ?
            sourcePartitionType.get("SUBPARTITIONING_TYPE").toString() : null;
        String targetSubType = targetPartitionType.get("SUBPARTITIONING_TYPE") != null ?
            targetPartitionType.get("SUBPARTITIONING_TYPE").toString() : null;

        boolean match = sourceType.equals(targetType) && Objects.equals(sourceSubType, targetSubType);
        resultBuilder.partitionStrategyMatch(match);
        if (!match) {
            resultBuilder.partitionStrategyMismatch(String.format(
                "Source: %s/%s, Target: %s/%s",
                sourceType, sourceSubType, targetType, targetSubType
            ));
        }
    }

    private void validateColumnOrder(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        List<Map<String, Object>> sourceColumns = sourceJdbcTemplate.queryForList(
            MetadataQueries.GET_COLUMNS,
            request.getSourceTable()
        );
        List<Map<String, Object>> targetColumns = targetJdbcTemplate.queryForList(
            MetadataQueries.GET_COLUMNS,
            request.getTargetTable()
        );

        List<String> sourceOrder = sourceColumns.stream()
            .map(col -> col.get("COLUMN_NAME").toString())
            .collect(Collectors.toList());
        List<String> targetOrder = targetColumns.stream()
            .map(col -> col.get("COLUMN_NAME").toString())
            .collect(Collectors.toList());

        boolean match = sourceOrder.equals(targetOrder);
        resultBuilder.columnOrderMatch(match);
        if (!match) {
            resultBuilder.columnOrderMismatch(String.format(
                "Source order: %s, Target order: %s",
                sourceOrder, targetOrder
            ));
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
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String resultPath = validationHistoryPath + "/" + result.getSourceTable() + "_" + timestamp + ".json";
        try {
            // TODO: Implement saving validation result to file or database
        } catch (Exception e) {
            log.error("Failed to save validation result", e);
        }
    }

    private void validatePartitions(ValidationRequest request, ValidationResult.ValidationResultBuilder resultBuilder) {
        Map<String, Long> sourcePartitionCounts = new HashMap<>();
        Map<String, Long> targetPartitionCounts = new HashMap<>();
        List<String> mismatches = new ArrayList<>();

        for (String partition : request.getPartitions()) {
            // Get row count for each partition
            Long sourceCount = sourceJdbcTemplate.queryForObject(
                String.format(MetadataQueries.GET_PARTITION_ROW_COUNT, 
                    request.getSourceTable(), 
                    request.getPartitionKey(), 
                    partition),
                Long.class
            );
            Long targetCount = targetJdbcTemplate.queryForObject(
                String.format(MetadataQueries.GET_PARTITION_ROW_COUNT, 
                    request.getTargetTable(), 
                    request.getPartitionKey(), 
                    partition),
                Long.class
            );

            sourcePartitionCounts.put(partition, sourceCount);
            targetPartitionCounts.put(partition, targetCount);

            if (!Objects.equals(sourceCount, targetCount)) {
                mismatches.add(String.format("Partition %s: Source=%d, Target=%d", 
                    partition, sourceCount, targetCount));
            }
        }

        resultBuilder.sourcePartitionCounts(sourcePartitionCounts)
                .targetPartitionCounts(targetPartitionCounts)
                .partitionMismatches(mismatches);
    }
} 