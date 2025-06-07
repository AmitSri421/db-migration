package com.example.dbmigration.service.impl;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import com.example.dbmigration.service.MetadataValidationService;
import com.example.dbmigration.util.MetadataQueries;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MetadataValidationServiceImpl implements MetadataValidationService {

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;

    public MetadataValidationServiceImpl(JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
    }

    @Override
    public ValidationResult validateMetadata(ValidationRequest request) {
        ValidationResult result = new ValidationResult();
        result.setSourceTable(request.getSourceTable());
        result.setTargetTable(request.getTargetTable());
        result.setValidationType(request.getValidationType());
        result.setStartTime(new Date());

        try {
            // Validate row counts
            if (request.isValidateRowCount()) {
                validateRowCounts(request, result);
            }

            // Validate indexes
            if (request.isValidateIndexes()) {
                validateIndexes(request, result);
            }

            // Validate constraints
            if (request.isValidateConstraints()) {
                validatePrimaryKeys(request, result);
                validateForeignKeys(request, result);
                validateUniqueKeys(request, result);
            }

            // Validate null/not-null constraints
            if (request.isValidateNullConstraints()) {
                validateNullConstraints(request, result);
            }

            // Validate data types and default values
            if (request.isValidateDataTypes()) {
                validateDataTypes(request, result);
            }

            // Validate partition strategy
            if (request.isValidatePartitionStrategy()) {
                validatePartitionStrategy(request, result);
            }

            // Validate column order
            if (request.isValidateColumnOrder()) {
                validateColumnOrder(request, result);
            }

            result.setSuccess(true);
        } catch (Exception e) {
            log.error("Error during metadata validation", e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        result.setEndTime(new Date());
        return result;
    }

    private void validateRowCounts(ValidationRequest request, ValidationResult result) {
        Long sourceCount = sourceJdbcTemplate.queryForObject(
            String.format(MetadataQueries.GET_ROW_COUNT, request.getSourceTable()),
            Long.class
        );
        Long targetCount = targetJdbcTemplate.queryForObject(
            String.format(MetadataQueries.GET_ROW_COUNT, request.getTargetTable()),
            Long.class
        );

        result.setSourceRowCount(sourceCount);
        result.setTargetRowCount(targetCount);
        result.setRowCountMatch(Objects.equals(sourceCount, targetCount));
    }

    private void validateIndexes(ValidationRequest request, ValidationResult result) {
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

        result.setMissingIndexes(new ArrayList<>(sourceIndexSet.stream()
            .filter(index -> !targetIndexSet.contains(index))
            .collect(Collectors.toSet())));
        result.setExtraIndexes(new ArrayList<>(targetIndexSet.stream()
            .filter(index -> !sourceIndexSet.contains(index))
            .collect(Collectors.toSet())));
    }

    private void validatePrimaryKeys(ValidationRequest request, ValidationResult result) {
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

        result.setPrimaryKeyMatch(sourcePKs.equals(targetPKs));
        if (!result.isPrimaryKeyMatch()) {
            result.setPrimaryKeyMismatch("Source PKs: " + sourcePKs + ", Target PKs: " + targetPKs);
        }
    }

    private void validateForeignKeys(ValidationRequest request, ValidationResult result) {
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

        result.setMissingForeignKeys(new ArrayList<>(sourceFKSet.stream()
            .filter(fk -> !targetFKSet.contains(fk))
            .collect(Collectors.toSet())));
        result.setExtraForeignKeys(new ArrayList<>(targetFKSet.stream()
            .filter(fk -> !sourceFKSet.contains(fk))
            .collect(Collectors.toSet())));
    }

    private void validateUniqueKeys(ValidationRequest request, ValidationResult result) {
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

        result.setUniqueKeyMatch(sourceUKs.equals(targetUKs));
        if (!result.isUniqueKeyMatch()) {
            result.setUniqueKeyMismatch("Source UKs: " + sourceUKs + ", Target UKs: " + targetUKs);
        }
    }

    private void validateNullConstraints(ValidationRequest request, ValidationResult result) {
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

        result.setNullConstraintMismatches(mismatches);
    }

    private void validateDataTypes(ValidationRequest request, ValidationResult result) {
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

        result.setDataTypeMismatches(mismatches);
    }

    private void validatePartitionStrategy(ValidationRequest request, ValidationResult result) {
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

        result.setPartitionStrategyMatch(
            sourceType.equals(targetType) &&
            Objects.equals(sourceSubType, targetSubType)
        );

        if (!result.isPartitionStrategyMatch()) {
            result.setPartitionStrategyMismatch(String.format(
                "Source: %s/%s, Target: %s/%s",
                sourceType, sourceSubType, targetType, targetSubType
            ));
        }
    }

    private void validateColumnOrder(ValidationRequest request, ValidationResult result) {
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

        result.setColumnOrderMatch(sourceOrder.equals(targetOrder));
        if (!result.isColumnOrderMatch()) {
            result.setColumnOrderMismatch(String.format(
                "Source order: %s, Target order: %s",
                sourceOrder, targetOrder
            ));
        }
    }
} 