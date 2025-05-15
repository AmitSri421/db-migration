package com.example.dbmigration.model;

import lombok.Data;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@Data
public class PartitionMapping {
    @NotBlank(message = "Source table name is required")
    private String sourceTable;
    
    @NotBlank(message = "Target table name is required")
    private String targetTable;
    
    @NotBlank(message = "Partition key is required")
    private String partitionKey;
    
    @Min(value = 1, message = "Batch size must be at least 1")
    private int batchSize;
    
    private String whereClause;
} 