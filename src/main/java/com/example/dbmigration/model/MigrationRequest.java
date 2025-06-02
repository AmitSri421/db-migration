package com.example.dbmigration.model;

import lombok.Data;
import java.util.List;

@Data
public class MigrationRequest {
    private String sourceTable;
    private String targetTable;
    private List<ColumnMapping> columnMappings;
    private String partitionKey;  // Optional, for partition-based migration
    private String partitionValue; // Optional, for partition-based migration
    private String whereClause;   // Optional, for filtering data
    private int batchSize = 1000; // Default batch size
} 