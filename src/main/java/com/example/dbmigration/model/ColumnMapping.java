package com.example.dbmigration.model;

import lombok.Data;

@Data
public class ColumnMapping {
    private String sourceColumn;
    private String targetColumn;
    private String dataType;
} 