package com.example.dbmigration.model;

import lombok.Data;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

@Data
public class ValidationRequest {
    @NotBlank(message = "Source table name is required")
    private String sourceTable;
    
    @NotBlank(message = "Target table name is required")
    private String targetTable;
    
    private String partitionKey;
    private List<String> partitions;
    
    @NotNull(message = "Validation type is required")
    private ValidationType validationType;
    
    private List<String> columns;  // Optional list of columns to validate
    
    private String whereClause;    // Optional WHERE clause for validation
    
    @NotNull(message = "Output directory is required")
    private String outputDirectory;
    
    private boolean validateRowCount = true;
    private boolean validateData = true;
    private boolean validatePartitions = true;
    
    public enum ValidationType {
        TABLE,
        PARTITION,
        BOTH
    }
} 