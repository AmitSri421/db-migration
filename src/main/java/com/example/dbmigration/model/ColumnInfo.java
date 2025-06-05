package com.example.dbmigration.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ColumnInfo {
    private String name;
    private String dataType;
    private int length;
    private int precision;
    private int scale;
} 