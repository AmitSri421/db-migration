package com.example.dbmigration.util;

import com.example.dbmigration.model.ColumnInfo;
import java.util.List;
import java.util.stream.Collectors;

public final class SqlBuilder {
    private SqlBuilder() {
        // Prevent instantiation
    }

    public static String buildSelectSql(String tableName, List<ColumnInfo> columns, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columns.stream().map(ColumnInfo::getName).collect(Collectors.toList())));
        sql.append(" FROM ").append(tableName);
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }

    public static String buildPartitionSelectSql(String tableName, List<ColumnInfo> columns, 
            String partitionKey, String partition, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columns.stream().map(ColumnInfo::getName).collect(Collectors.toList())));
        sql.append(" FROM ").append(tableName);
        sql.append(" PARTITION(").append(partition).append(")");
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }

    public static String buildInsertSql(String tableName, List<ColumnInfo> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (").append(String.join(", ", columns.stream().map(ColumnInfo::getName).collect(Collectors.toList()))).append(") ");
        sql.append("VALUES (");
        sql.append("?, ".repeat(columns.size() - 1)).append("?)");
        return sql.toString();
    }

    public static String buildTruncateSql(String tableName, String partitionValue, boolean cascade) {
        StringBuilder sql = new StringBuilder("TRUNCATE TABLE ").append(tableName);
        
        if (partitionValue != null && !partitionValue.isEmpty()) {
            sql.append(" PARTITION(").append(partitionValue).append(")");
        }
        
        if (cascade) {
            sql.append(" CASCADE");
        }
        
        return sql.toString();
    }

    public static String buildDeleteSql(String tableName, String partitionValue, String whereClause) {
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
        
        if (partitionValue != null && !partitionValue.isEmpty()) {
            sql.append(" PARTITION(").append(partitionValue).append(")");
        }
        
        sql.append(" WHERE ").append(whereClause);
        
        return sql.toString();
    }
} 