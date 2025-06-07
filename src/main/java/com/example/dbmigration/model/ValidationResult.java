package com.example.dbmigration.model;

import lombok.Data;
import lombok.Builder;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class ValidationResult {
    private String sourceTable;
    private String targetTable;
    private ValidationType validationType;
    private Date startTime;
    private Date endTime;
    private boolean success;
    private String errorMessage;

    // Row count validation
    private Long sourceRowCount;
    private Long targetRowCount;
    private boolean rowCountMatch;
    private Map<String, Long> partitionRowCounts;

    // Index validation
    private List<String> missingIndexes;
    private List<String> extraIndexes;
    private List<String> differentIndexes;

    // Constraint validation
    private List<String> missingPrimaryKeys;
    private List<String> extraPrimaryKeys;
    private List<String> missingForeignKeys;
    private List<String> extraForeignKeys;
    private List<String> missingUniqueKeys;
    private List<String> extraUniqueKeys;

    // Null/not-null validation
    private List<String> nullConstraintMismatches;
    private List<String> notNullConstraintMismatches;

    // Data type validation
    private List<String> dataTypeMismatches;
    private List<String> defaultValueMismatches;

    // Partition validation
    private Map<String, Long> sourcePartitionCounts;
    private Map<String, Long> targetPartitionCounts;
    private List<String> partitionMismatches;

    // Partition strategy validation
    private boolean partitionStrategyMatch;
    private String partitionStrategyMismatch;
    private List<String> partitionStrategyMismatches;

    // Column order validation
    private boolean columnOrderMatch;
    private String columnOrderMismatch;
    private List<String> columnOrderMismatches;
} 