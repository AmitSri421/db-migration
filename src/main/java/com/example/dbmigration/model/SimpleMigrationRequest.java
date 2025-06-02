package com.example.dbmigration.model;

import lombok.Data;
import java.util.List;

@Data
public class SimpleMigrationRequest {
    private String sourceTable;
    private String targetTable;
    private List<String> columns;  // List of column names (same for source and target)
    private String partitionKey;   // Optional, for partition-based migration
    private String partitionValue; // Optional, for partition-based migration
    private String whereClause;    // Optional, for filtering data
    private int batchSize = 1000;  // Default batch size
} 