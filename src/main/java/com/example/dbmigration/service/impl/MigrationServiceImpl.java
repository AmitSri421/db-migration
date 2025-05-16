package com.example.dbmigration.service.impl;

import com.example.dbmigration.config.MappingConfig;
import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;
import com.example.dbmigration.service.MigrationService;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MigrationServiceImpl implements MigrationService {

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;
    private final MappingConfig mappingConfig;

    public MigrationServiceImpl(
            @Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate,
            MappingConfig mappingConfig) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.mappingConfig = mappingConfig;
    }

    @Override
    @Transactional
    public void migrateTable(TableMapping mapping) {
        log.info("Starting migration for table: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
        
        try {
            // Get column information with data types
            List<ColumnInfo> columns = getTableColumns(mapping.getSourceTable());
            
            // Build SQL statements
            String selectSql = buildSelectSql(mapping.getSourceTable(), columns, mapping.getWhereClause());
            String insertSql = buildInsertSql(mapping.getTargetTable(), columns);
            
            // Process in batches
            int batchSize = mapping.getBatchSize();
            List<Map<String, Object>> batch = new ArrayList<>();
            
            try (Connection sourceConn = sourceJdbcTemplate.getDataSource().getConnection();
                 PreparedStatement ps = sourceConn.prepareStatement(selectSql);
                 ResultSet rs = ps.executeQuery()) {
                
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (ColumnInfo column : columns) {
                        row.put(column.getName(), getColumnValue(rs, column));
                    }
                    batch.add(row);
                    
                    if (batch.size() >= batchSize) {
                        processBatch(insertSql, batch, columns);
                        batch.clear();
                    }
                }
                
                // Process remaining records
                if (!batch.isEmpty()) {
                    processBatch(insertSql, batch, columns);
                }
            }
            
            log.info("Completed migration for table: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error migrating table: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable(), e);
            throw new RuntimeException("Migration failed", e);
        }
    }

    @Override
    @Transactional
    public void migratePartition(PartitionMapping mapping) {
        log.info("Starting migration for partition: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
        
        try {
            // Get partition information
            List<String> partitions = getPartitions(mapping.getSourceTable(), mapping.getPartitionKey());
            
            for (String partition : partitions) {
                log.info("Migrating partition: {}", partition);
                
                // Get column information with data types
                List<ColumnInfo> columns = getTableColumns(mapping.getSourceTable());
                
                // Build SQL statements with partition
                String selectSql = buildPartitionSelectSql(mapping.getSourceTable(), columns, 
                    mapping.getPartitionKey(), partition, mapping.getWhereClause());
                String insertSql = buildInsertSql(mapping.getTargetTable(), columns);
                
                // Process in batches
                int batchSize = mapping.getBatchSize();
                List<Map<String, Object>> batch = new ArrayList<>();
                
                try (Connection sourceConn = sourceJdbcTemplate.getDataSource().getConnection();
                     PreparedStatement ps = sourceConn.prepareStatement(selectSql);
                     ResultSet rs = ps.executeQuery()) {
                    
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (ColumnInfo column : columns) {
                            row.put(column.getName(), getColumnValue(rs, column));
                        }
                        batch.add(row);
                        
                        if (batch.size() >= batchSize) {
                            processBatch(insertSql, batch, columns);
                            batch.clear();
                        }
                    }
                    
                    // Process remaining records
                    if (!batch.isEmpty()) {
                        processBatch(insertSql, batch, columns);
                    }
                }
            }
            
            log.info("Completed migration for partition: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error migrating partition: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable(), e);
            throw new RuntimeException("Migration failed", e);
        }
    }

    @Override
    public void migrateAllTables() {
        List<TableMapping> tables = mappingConfig.getTables();
        for (TableMapping table : tables) {
            try {
                migrateTable(table);
            } catch (Exception e) {
                log.error("Failed to migrate table: {} -> {}", table.getSourceTable(), table.getTargetTable(), e);
                // Continue with next table
            }
        }
    }

    @Override
    public void migrateAllPartitions() {
        List<PartitionMapping> partitions = mappingConfig.getPartitions();
        for (PartitionMapping partition : partitions) {
            try {
                migratePartition(partition);
            } catch (Exception e) {
                log.error("Failed to migrate partition: {} -> {}", 
                    partition.getSourceTable(), partition.getTargetTable(), e);
                // Continue with next partition
            }
        }
    }

    @Override
    public void migrateAll() {
        migrateAllTables();
        migrateAllPartitions();
    }

    private List<ColumnInfo> getTableColumns(String tableName) {
        String sql = "SELECT column_name, data_type, data_length, data_precision, data_scale " +
                    "FROM all_tab_columns WHERE table_name = ? ORDER BY column_id";
        return sourceJdbcTemplate.query(sql, (rs, rowNum) -> {
            ColumnInfo column = new ColumnInfo();
            column.setName(rs.getString("column_name"));
            column.setDataType(rs.getString("data_type"));
            column.setLength(rs.getInt("data_length"));
            column.setPrecision(rs.getInt("data_precision"));
            column.setScale(rs.getInt("data_scale"));
            return column;
        }, tableName);
    }

    private List<String> getPartitions(String tableName, String partitionKey) {
        String sql = "SELECT partition_name FROM all_tab_partitions WHERE table_name = ? AND partition_key_column LIKE ?";
        return sourceJdbcTemplate.queryForList(sql, String.class, tableName, "%" + partitionKey + "%");
    }

    private String buildSelectSql(String tableName, List<ColumnInfo> columns, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columns.stream().map(ColumnInfo::getName).collect(Collectors.toList())));
        sql.append(" FROM ").append(tableName);
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }

    private String buildPartitionSelectSql(String tableName, List<ColumnInfo> columns, 
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

    private String buildInsertSql(String tableName, List<ColumnInfo> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (").append(String.join(", ", columns.stream().map(ColumnInfo::getName).collect(Collectors.toList()))).append(") ");
        sql.append("VALUES (");
        sql.append("?, ".repeat(columns.size() - 1)).append("?)");
        return sql.toString();
    }

    private Object getColumnValue(ResultSet rs, ColumnInfo column) throws SQLException {
        String dataType = column.getDataType();
        String columnName = column.getName();
        
        switch (dataType) {
            case "NUMBER":
                if (column.getScale() > 0) {
                    return rs.getBigDecimal(columnName);
                } else {
                    return rs.getLong(columnName);
                }
            case "VARCHAR2":
                return rs.getString(columnName);
            case "TIMESTAMP(6)":
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

    private void processBatch(String insertSql, List<Map<String, Object>> batch, List<ColumnInfo> columns) {
        targetJdbcTemplate.batchUpdate(insertSql, batch, batch.size(), (ps, row) -> {
            int i = 1;
            for (ColumnInfo column : columns) {
                Object value = row.get(column.getName());
                if (value == null) {
                    ps.setNull(i++, getSqlType(column.getDataType()));
                } else {
                    setParameterValue(ps, i++, value, column);
                }
            }
        });
    }

    private int getSqlType(String dataType) {
        switch (dataType) {
            case "NUMBER":
                return Types.NUMERIC;
            case "VARCHAR2":
                return Types.VARCHAR;
            case "TIMESTAMP(6)":
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

    private void setParameterValue(PreparedStatement ps, int index, Object value, ColumnInfo column) throws SQLException {
        String dataType = column.getDataType();
        
        switch (dataType) {
            case "NUMBER":
                if (value instanceof Number) {
                    ps.setObject(index, value);
                } else {
                    ps.setNull(index, Types.NUMERIC);
                }
                break;
            case "VARCHAR2":
                ps.setString(index, (String) value);
                break;
            case "TIMESTAMP(6)":
                ps.setTimestamp(index, (Timestamp) value);
                break;
            case "DATE":
                ps.setDate(index, (Date) value);
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

    @Getter
    @Setter
    private static class ColumnInfo {
        private String name;
        private String dataType;
        private int length;
        private int precision;
        private int scale;
    }
} 