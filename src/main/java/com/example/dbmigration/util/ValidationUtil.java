package com.example.dbmigration.util;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public final class ValidationUtil {
    private ValidationUtil() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static long getRowCount(JdbcTemplate jdbcTemplate, String tableName, String whereClause) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sql += " WHERE " + whereClause;
        }
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    public static List<String> getPartitions(JdbcTemplate jdbcTemplate, String tableName) {
        String sql = "SELECT partition_name FROM user_tab_partitions WHERE table_name = ?";
        return jdbcTemplate.queryForList(sql, String.class, tableName.toUpperCase());
    }

    public static Map<String, Long> compareColumnData(
            JdbcTemplate sourceJdbcTemplate,
            JdbcTemplate targetJdbcTemplate,
            String sourceTable,
            String targetTable,
            List<String> columns,
            String whereClause) {
        
        Map<String, Long> mismatchCounts = new HashMap<>();
        StringBuilder columnList = new StringBuilder();
        for (String column : columns) {
            if (columnList.length() > 0) {
                columnList.append(", ");
            }
            columnList.append(column);
        }

        String sql = String.format(
            "SELECT %s, COUNT(*) as count FROM %s %s GROUP BY %s",
            columnList,
            sourceTable,
            whereClause != null ? "WHERE " + whereClause : "",
            columnList
        );

        // Get source data
        Map<String, Long> sourceData = new HashMap<>();
        sourceJdbcTemplate.query(sql, rs -> {
            StringBuilder key = new StringBuilder();
            for (String column : columns) {
                if (key.length() > 0) {
                    key.append("|");
                }
                key.append(rs.getString(column));
            }
            sourceData.put(key.toString(), rs.getLong("count"));
        });

        // Get target data and compare
        targetJdbcTemplate.query(sql.replace(sourceTable, targetTable), rs -> {
            StringBuilder key = new StringBuilder();
            for (String column : columns) {
                if (key.length() > 0) {
                    key.append("|");
                }
                key.append(rs.getString(column));
            }
            String dataKey = key.toString();
            Long sourceCount = sourceData.get(dataKey);
            Long targetCount = rs.getLong("count");
            
            if (sourceCount == null || !sourceCount.equals(targetCount)) {
                mismatchCounts.put(dataKey, Math.abs((sourceCount != null ? sourceCount : 0) - targetCount));
            }
        });

        return mismatchCounts;
    }

    public static String formatDuration(Date startTime, Date endTime) {
        long duration = endTime.getTime() - startTime.getTime();
        long seconds = duration / 1000;
        long minutes = seconds / 60;
        seconds = seconds % 60;
        return String.format("%d minutes, %d seconds", minutes, seconds);
    }

    public static void writeValidationReport(List<ValidationResult> results, String outputPath) throws IOException {
        StringBuilder report = new StringBuilder();
        report.append("Validation Report\n");
        report.append("================\n\n");
        report.append("Generated: ").append(DATE_FORMAT.format(new Date())).append("\n\n");

        for (ValidationResult result : results) {
            report.append("Table: ").append(result.getSourceTable()).append(" -> ").append(result.getTargetTable()).append("\n");
            report.append("Validation Type: ").append(result.getValidationType()).append("\n");
            report.append("Start Time: ").append(DATE_FORMAT.format(result.getStartTime())).append("\n");
            report.append("End Time: ").append(DATE_FORMAT.format(result.getEndTime())).append("\n");
            report.append("Duration: ").append(formatDuration(result.getStartTime(), result.getEndTime())).append("\n");
            report.append("Success: ").append(result.isSuccess()).append("\n\n");

            if (!result.isSuccess()) {
                report.append("Error: ").append(result.getErrorMessage()).append("\n\n");
                continue;
            }

            // Row count validation
            if (result.getSourceRowCount() != null) {
                report.append("Row Count Validation:\n");
                report.append("  Source: ").append(result.getSourceRowCount()).append("\n");
                report.append("  Target: ").append(result.getTargetRowCount()).append("\n");
                report.append("  Match: ").append(result.isRowCountMatch()).append("\n\n");
            }

            // Index validation
            if (result.getMissingIndexes() != null || result.getExtraIndexes() != null) {
                report.append("Index Validation:\n");
                if (result.getMissingIndexes() != null && !result.getMissingIndexes().isEmpty()) {
                    report.append("  Missing Indexes: ").append(result.getMissingIndexes()).append("\n");
                }
                if (result.getExtraIndexes() != null && !result.getExtraIndexes().isEmpty()) {
                    report.append("  Extra Indexes: ").append(result.getExtraIndexes()).append("\n");
                }
                report.append("\n");
            }

            // Constraint validation
            report.append("Constraint Validation:\n");
            if (result.getMissingPrimaryKeys() != null && !result.getMissingPrimaryKeys().isEmpty()) {
                report.append("  Missing Primary Keys: ").append(result.getMissingPrimaryKeys()).append("\n");
            }
            if (result.getExtraPrimaryKeys() != null && !result.getExtraPrimaryKeys().isEmpty()) {
                report.append("  Extra Primary Keys: ").append(result.getExtraPrimaryKeys()).append("\n");
            }
            if (result.getMissingForeignKeys() != null && !result.getMissingForeignKeys().isEmpty()) {
                report.append("  Missing Foreign Keys: ").append(result.getMissingForeignKeys()).append("\n");
            }
            if (result.getExtraForeignKeys() != null && !result.getExtraForeignKeys().isEmpty()) {
                report.append("  Extra Foreign Keys: ").append(result.getExtraForeignKeys()).append("\n");
            }
            if (result.getMissingUniqueKeys() != null && !result.getMissingUniqueKeys().isEmpty()) {
                report.append("  Missing Unique Keys: ").append(result.getMissingUniqueKeys()).append("\n");
            }
            if (result.getExtraUniqueKeys() != null && !result.getExtraUniqueKeys().isEmpty()) {
                report.append("  Extra Unique Keys: ").append(result.getExtraUniqueKeys()).append("\n");
            }
            report.append("\n");

            // Null constraint validation
            if (result.getNullConstraintMismatches() != null && !result.getNullConstraintMismatches().isEmpty()) {
                report.append("Null Constraint Validation:\n");
                report.append("  Mismatches: ").append(result.getNullConstraintMismatches()).append("\n\n");
            }

            // Data type validation
            if (result.getDataTypeMismatches() != null && !result.getDataTypeMismatches().isEmpty()) {
                report.append("Data Type Validation:\n");
                report.append("  Mismatches: ").append(result.getDataTypeMismatches()).append("\n\n");
            }

            // Default value validation
            if (result.getDefaultValueMismatches() != null && !result.getDefaultValueMismatches().isEmpty()) {
                report.append("Default Value Validation:\n");
                report.append("  Mismatches: ").append(result.getDefaultValueMismatches()).append("\n\n");
            }

            // Partition strategy validation
            report.append("Partition Strategy Validation:\n");
            report.append("  Match: ").append(result.isPartitionStrategyMatch()).append("\n");
            if (!result.isPartitionStrategyMatch() && result.getPartitionStrategyMismatch() != null) {
                report.append("  Mismatch: ").append(result.getPartitionStrategyMismatch()).append("\n");
            }
            report.append("\n");

            // Column order validation
            report.append("Column Order Validation:\n");
            report.append("  Match: ").append(result.isColumnOrderMatch()).append("\n");
            if (!result.isColumnOrderMatch() && result.getColumnOrderMismatch() != null) {
                report.append("  Mismatch: ").append(result.getColumnOrderMismatch()).append("\n");
            }
            report.append("\n");

            report.append("----------------------------------------\n\n");
        }

        Path path = Paths.get(outputPath);
        Files.createDirectories(path.getParent());
        Files.write(path, report.toString().getBytes());
    }
} 