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
    
    @NotBlank(message = "Output directory is required")
    private String outputDirectory;
    
    // Metadata validations
    private boolean validateRowCount = true;
    private boolean validateIndexes = true;
    private boolean validateConstraints = true;
    private boolean validateNullConstraints = true;
    private boolean validateDataTypes = true;
    private boolean validateDefaultValues = true;
    private boolean validatePartitionStrategy = true;
    private boolean validateColumnOrder = true;
    
    public enum ValidationType {
        TABLE,
        PARTITION,
        BOTH
    }
} 