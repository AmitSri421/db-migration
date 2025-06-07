package com.example.dbmigration.model;

import lombok.Data;
import lombok.Builder;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class ValidationResult {
    private String sourceTable;
    private String targetTable;
    private String partitionName;
    private ValidationRequest.ValidationType validationType;
    
    private long sourceRowCount;
    private long targetRowCount;
    private boolean rowCountMatch;
    
    private List<String> mismatchedColumns;
    private Map<String, Long> columnMismatchCounts;
    
    private List<String> missingPartitions;
    private List<String> extraPartitions;
    
    private LocalDateTime validationStartTime;
    private LocalDateTime validationEndTime;
    private String validationDuration;
    
    private String errorMessage;
    private boolean success;
    
    @Data
    @Builder
    public static class ColumnMismatch {
        private String columnName;
        private long mismatchCount;
        private String sampleMismatch;  // Sample of mismatched data
    }
} 