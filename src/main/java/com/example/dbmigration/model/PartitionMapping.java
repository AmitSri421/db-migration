package com.example.dbmigration.model;

import lombok.Data;

@Data
public class PartitionMapping {
    private String sourceTable;
    private String targetTable;
    private String partitionKey;
    private int batchSize;
    private String whereClause;
} 