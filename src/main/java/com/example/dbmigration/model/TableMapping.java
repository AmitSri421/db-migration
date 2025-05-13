package com.example.dbmigration.model;

import lombok.Data;

@Data
public class TableMapping {
    private String sourceTable;
    private String targetTable;
    private int batchSize;
    private String whereClause;
} 