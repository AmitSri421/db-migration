package com.example.dbmigration.util;

import com.example.dbmigration.model.ColumnInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class BatchProcessor {
    private BatchProcessor() {
        // Prevent instantiation
    }

    public static void processBatch(String sql, List<Map<String, Object>> batch, List<ColumnInfo> columns, 
            String tableName, JdbcTemplate jdbcTemplate, Consumer<Map<String, Object>> errorHandler) {
        jdbcTemplate.batchUpdate(sql, batch, batch.size(), (ps, row) -> {
            try {
                int i = 1;
                for (ColumnInfo column : columns) {
                    Object value = row.get(column.getName());
                    if (value == null) {
                        ps.setNull(i++, RowMapperUtil.getSqlType(column.getDataType()));
                    } else {
                        RowMapperUtil.setParameterValue(ps, i++, value, column);
                    }
                }
            } catch (SQLException e) {
                log.error("Failed to process batch for table {}: {}", tableName, e.getMessage());
                if (errorHandler != null) {
                    errorHandler.accept(row);
                }
                throw new RuntimeException("Failed to set parameter values", e);
            }
        });
    }

    public static int processDeleteBatch(String deleteSql, int batchSize, JdbcTemplate jdbcTemplate) {
        int totalDeleted = 0;
        while (true) {
            String batchDeleteSql = deleteSql + " AND ROWNUM <= " + batchSize;
            int deleted = jdbcTemplate.update(batchDeleteSql);
            totalDeleted += deleted;
            
            if (deleted < batchSize) {
                break;
            }
        }
        return totalDeleted;
    }
} 