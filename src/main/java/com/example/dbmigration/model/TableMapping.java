package com.example.dbmigration.model;

import lombok.Data;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

@Data
public class TableMapping {
    @NotBlank(message = "Source table name is required")
    private String sourceTable;
    
    @NotBlank(message = "Target table name is required")
    private String targetTable;
    
    @Min(value = 1, message = "Batch size must be at least 1")
    private int batchSize;
    
    private String whereClause;
} 