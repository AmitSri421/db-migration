package com.example.dbmigration.service.impl;

import com.example.dbmigration.model.ColumnMapping;
import com.example.dbmigration.model.MigrationRequest;
import com.example.dbmigration.service.ColumnAwareMigrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ColumnAwareMigrationServiceImpl implements ColumnAwareMigrationService {

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;
    private final String failedRecordsDir;

    public ColumnAwareMigrationServiceImpl(
            @Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate,
            @Value("${app.migration.output.failed-records-dir}") String failedRecordsDir) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.failedRecordsDir = failedRecordsDir;
        createFailedRecordsDirectory();
    }

    private void createFailedRecordsDirectory() {
        try {
            Path dir = Paths.get(failedRecordsDir);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
        } catch (IOException e) {
            log.error("Failed to create failed records directory: {}", failedRecordsDir, e);
        }
    }

    @Override
    @Transactional
    public void migrate(MigrationRequest request) {
        log.info("Starting migration for table: {} -> {}", request.getSourceTable(), request.getTargetTable());
        
        try {
            String selectSql = buildSelectSql(request);
            String insertSql = buildInsertSql(request);
            
            List<Map<String, Object>> batch = new ArrayList<>();
            
            try (Connection sourceConn = sourceJdbcTemplate.getDataSource().getConnection();
                 PreparedStatement ps = sourceConn.prepareStatement(selectSql);
                 ResultSet rs = ps.executeQuery()) {
                
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (ColumnMapping mapping : request.getColumnMappings()) {
                        row.put(mapping.getTargetColumn(), getColumnValue(rs, mapping));
                    }
                    batch.add(row);
                    
                    if (batch.size() >= request.getBatchSize()) {
                        processBatch(insertSql, batch, request);
                        batch.clear();
                    }
                }
                
                if (!batch.isEmpty()) {
                    processBatch(insertSql, batch, request);
                }
            }
            
            log.info("Completed migration for table: {} -> {}", request.getSourceTable(), request.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error migrating table: {} -> {}", request.getSourceTable(), request.getTargetTable(), e);
            throw new RuntimeException("Migration failed", e);
        }
    }

    private String buildSelectSql(MigrationRequest request) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(request.getColumnMappings().stream()
                .map(ColumnMapping::getSourceColumn)
                .collect(Collectors.joining(", ")));
        sql.append(" FROM ").append(request.getSourceTable());
        
        if (request.getPartitionKey() != null && request.getPartitionValue() != null) {
            sql.append(" PARTITION(").append(request.getPartitionValue()).append(")");
        }
        
        if (request.getWhereClause() != null && !request.getWhereClause().isEmpty()) {
            sql.append(" WHERE ").append(request.getWhereClause());
        }
        
        return sql.toString();
    }

    private String buildInsertSql(MigrationRequest request) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(request.getTargetTable());
        sql.append(" (").append(request.getColumnMappings().stream()
                .map(ColumnMapping::getTargetColumn)
                .collect(Collectors.joining(", "))).append(") ");
        sql.append("VALUES (");
        sql.append("?, ".repeat(request.getColumnMappings().size() - 1)).append("?)");
        return sql.toString();
    }

    private Object getColumnValue(ResultSet rs, ColumnMapping mapping) throws SQLException {
        String dataType = mapping.getDataType();
        String columnName = mapping.getSourceColumn();
        
        switch (dataType.toUpperCase()) {
            case "NUMBER":
                return rs.getBigDecimal(columnName);
            case "VARCHAR2":
            case "VARCHAR":
            case "CHAR":
                return rs.getString(columnName);
            case "TIMESTAMP":
                return rs.getTimestamp(columnName);
            case "DATE":
                return rs.getDate(columnName);
            case "BLOB":
                Blob blob = rs.getBlob(columnName);
                return blob != null ? blob.getBytes(1, (int) blob.length()) : null;
            case "CLOB":
                Clob clob = rs.getClob(columnName);
                return clob != null ? clob.getSubString(1, (int) clob.length()) : null;
            default:
                return rs.getObject(columnName);
        }
    }

    private void processBatch(String insertSql, List<Map<String, Object>> batch, MigrationRequest request) {
        String failedRecordsFile = getFailedRecordsFileName(request.getTargetTable());
        
        targetJdbcTemplate.batchUpdate(insertSql, batch, batch.size(), (ps, row) -> {
            try {
                int i = 1;
                for (ColumnMapping mapping : request.getColumnMappings()) {
                    Object value = row.get(mapping.getTargetColumn());
                    if (value == null) {
                        ps.setNull(i++, getSqlType(mapping.getDataType()));
                    } else {
                        setParameterValue(ps, i++, value, mapping.getDataType());
                    }
                }
            } catch (SQLException e) {
                logFailedRecord(failedRecordsFile, row, e.getMessage());
                throw new RuntimeException("Failed to set parameter values", e);
            }
        });
    }

    private int getSqlType(String dataType) {
        switch (dataType.toUpperCase()) {
            case "NUMBER":
                return Types.NUMERIC;
            case "VARCHAR2":
            case "VARCHAR":
            case "CHAR":
                return Types.VARCHAR;
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "DATE":
                return Types.DATE;
            case "BLOB":
                return Types.BLOB;
            case "CLOB":
                return Types.CLOB;
            default:
                return Types.OTHER;
        }
    }

    private void setParameterValue(PreparedStatement ps, int index, Object value, String dataType) throws SQLException {
        switch (dataType.toUpperCase()) {
            case "NUMBER":
                if (value instanceof Number) {
                    ps.setObject(index, value);
                } else {
                    ps.setNull(index, Types.NUMERIC);
                }
                break;
            case "VARCHAR2":
            case "VARCHAR":
            case "CHAR":
                ps.setString(index, (String) value);
                break;
            case "TIMESTAMP":
                ps.setTimestamp(index, (Timestamp) value);
                break;
            case "DATE":
                ps.setDate(index, (java.sql.Date) value);
                break;
            case "BLOB":
                byte[] blobData = (byte[]) value;
                if (blobData != null) {
                    ps.setBytes(index, blobData);
                } else {
                    ps.setNull(index, Types.BLOB);
                }
                break;
            case "CLOB":
                String clobData = (String) value;
                if (clobData != null) {
                    ps.setString(index, clobData);
                } else {
                    ps.setNull(index, Types.CLOB);
                }
                break;
            default:
                ps.setObject(index, value);
        }
    }

    private String getFailedRecordsFileName(String tableName) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("%s/%s_failed_records_%s.csv", failedRecordsDir, tableName, timestamp);
    }

    private void logFailedRecord(String fileName, Map<String, Object> record, String errorMessage) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            if (Files.size(Paths.get(fileName)) == 0) {
                writer.write("timestamp,error_message,record_data\n");
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            String recordData = record.entrySet().stream()
                    .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining("|"));
            
            writer.write(String.format("%s,%s,%s\n", timestamp, errorMessage, recordData));
        } catch (IOException e) {
            log.error("Failed to write failed record to file: {}", fileName, e);
        }
    }
} 