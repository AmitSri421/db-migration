package com.example.dbmigration.model;

import lombok.Data;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

@Data
public class DeleteRequest {
    @NotBlank(message = "Target table name is required")
    private String targetTable;
    
    private String partitionValue;  // Optional, for deleting from specific partition
    
    @NotBlank(message = "WHERE clause is required")
    private String whereClause;
    
    @Min(value = 1, message = "Batch size must be at least 1")
    private int batchSize = 1000;  // Default batch size
} 