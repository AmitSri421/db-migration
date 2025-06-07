package com.example.dbmigration.util;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.io.*;
import java.nio.file.*;

@Slf4j
public final class ValidationUtil {
    private ValidationUtil() {
        throw new AssertionError("Utility class should not be instantiated");
    }

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

    public static void writeValidationReport(List<ValidationResult> results, String outputPath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputPath))) {
            writer.write("Validation Report\n");
            writer.write("================\n\n");
            writer.write("Generated: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "\n\n");

            for (ValidationResult result : results) {
                writer.write("Table: " + result.getSourceTable() + " -> " + result.getTargetTable() + "\n");
                writer.write("Validation Type: " + result.getValidationType() + "\n");
                writer.write("Start Time: " + result.getValidationStartTime() + "\n");
                writer.write("End Time: " + result.getValidationEndTime() + "\n");
                writer.write("Duration: " + result.getValidationDuration() + "\n");
                writer.write("Success: " + result.isSuccess() + "\n\n");

                if (!result.isSuccess()) {
                    writer.write("Errors:\n");
                    writer.write(result.getErrorMessage() + "\n\n");
                }

                if (result.isRowCountMatch()) {
                    writer.write("Row Count: " + result.getSourceRowCount() + " (matches)\n");
                } else {
                    writer.write("Row Count Mismatch:\n");
                    writer.write("  Source: " + result.getSourceRowCount() + "\n");
                    writer.write("  Target: " + result.getTargetRowCount() + "\n");
                }

                if (result.getMismatchedColumns() != null && !result.getMismatchedColumns().isEmpty()) {
                    writer.write("\nMismatched Columns:\n");
                    for (String column : result.getMismatchedColumns()) {
                        writer.write("  - " + column + "\n");
                    }
                }

                if (result.getMissingPartitions() != null && !result.getMissingPartitions().isEmpty()) {
                    writer.write("\nMissing Partitions:\n");
                    for (String partition : result.getMissingPartitions()) {
                        writer.write("  - " + partition + "\n");
                    }
                }

                if (result.getExtraPartitions() != null && !result.getExtraPartitions().isEmpty()) {
                    writer.write("\nExtra Partitions:\n");
                    for (String partition : result.getExtraPartitions()) {
                        writer.write("  - " + partition + "\n");
                    }
                }

                writer.write("\n" + "=".repeat(50) + "\n\n");
            }
        }
    }
} 