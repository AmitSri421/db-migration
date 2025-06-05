package com.example.dbmigration.service.impl;

import com.example.dbmigration.config.MappingConfig;
import com.example.dbmigration.constant.MigrationConstants;
import com.example.dbmigration.exception.MigrationException;
import com.example.dbmigration.model.ColumnInfo;
import com.example.dbmigration.model.DeleteRequest;
import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;
import com.example.dbmigration.model.TruncateRequest;
import com.example.dbmigration.service.MigrationService;
import com.example.dbmigration.util.BatchProcessor;
import com.example.dbmigration.util.RowMapperUtil;
import com.example.dbmigration.util.SqlBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

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
public class MigrationServiceImpl implements MigrationService {

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;
    private final MappingConfig mappingConfig;
    private final String failedRecordsDir;

    public MigrationServiceImpl(
            @Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate,
            MappingConfig mappingConfig,
            @Value("${app.migration.output.failed-records-dir}") String failedRecordsDir) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.mappingConfig = mappingConfig;
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
            throw new MigrationException("Failed to create failed records directory", e);
        }
    }

    private String getFailedRecordsFileName(String tableName) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern(MigrationConstants.TIMESTAMP_FORMAT));
        return String.format(MigrationConstants.FAILED_RECORDS_FORMAT, failedRecordsDir, tableName, timestamp);
    }

    private void logFailedRecord(String fileName, Map<String, Object> record, String errorMessage) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            // Write header if file is new
            if (Files.size(Paths.get(fileName)) == 0) {
                writer.write(MigrationConstants.FAILED_RECORDS_HEADER);
            }

            // Write failed record
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern(MigrationConstants.ISO_TIMESTAMP_FORMAT));
            String recordData = record.entrySet().stream()
                    .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining("|"));
            
            writer.write(String.format("%s,%s,%s\n", timestamp, errorMessage, recordData));
        } catch (IOException e) {
            log.error("Failed to write failed record to file: {}", fileName, e);
            throw new MigrationException("Failed to write failed record", e);
        }
    }

    @Override
    @Transactional
    public void migrateTable(TableMapping mapping) {
        log.info("Starting migration for table: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
        
        try {
            // Get column information with data types
            List<ColumnInfo> columns = mapping.getColumns() != null && !mapping.getColumns().isEmpty() 
                ? getTableColumns(mapping.getSourceTable(), mapping.getColumns())
                : getTableColumns(mapping.getSourceTable());
            
            // Build SQL statements
            String selectSql = SqlBuilder.buildSelectSql(mapping.getSourceTable(), columns, mapping.getWhereClause());
            String insertSql = SqlBuilder.buildInsertSql(mapping.getTargetTable(), columns);
            
            // Process in batches
            int batchSize = Math.min(mapping.getBatchSize(), MigrationConstants.MAX_BATCH_SIZE);
            List<Map<String, Object>> batch = new ArrayList<>();
            
            try (Connection sourceConn = sourceJdbcTemplate.getDataSource().getConnection();
                 PreparedStatement ps = sourceConn.prepareStatement(selectSql);
                 ResultSet rs = ps.executeQuery()) {
                
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (ColumnInfo column : columns) {
                        row.put(column.getName(), RowMapperUtil.getColumnValue(rs, column));
                    }
                    batch.add(row);
                    
                    if (batch.size() >= batchSize) {
                        processBatch(insertSql, batch, columns, mapping.getTargetTable());
                        batch.clear();
                    }
                }
                
                // Process remaining records
                if (!batch.isEmpty()) {
                    processBatch(insertSql, batch, columns, mapping.getTargetTable());
                }
            }
            
            log.info("Completed migration for table: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error migrating table: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable(), e);
            throw new MigrationException("Migration failed", e);
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
                List<ColumnInfo> columns = mapping.getColumns() != null && !mapping.getColumns().isEmpty()
                    ? getTableColumns(mapping.getSourceTable(), mapping.getColumns())
                    : getTableColumns(mapping.getSourceTable());
                
                // Build SQL statements with partition
                String selectSql = SqlBuilder.buildPartitionSelectSql(mapping.getSourceTable(), columns, 
                    mapping.getPartitionKey(), partition, mapping.getWhereClause());
                String insertSql = SqlBuilder.buildInsertSql(mapping.getTargetTable(), columns);
                
                // Process in batches
                int batchSize = Math.min(mapping.getBatchSize(), MigrationConstants.MAX_BATCH_SIZE);
                List<Map<String, Object>> batch = new ArrayList<>();
                
                try (Connection sourceConn = sourceJdbcTemplate.getDataSource().getConnection();
                     PreparedStatement ps = sourceConn.prepareStatement(selectSql);
                     ResultSet rs = ps.executeQuery()) {
                    
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (ColumnInfo column : columns) {
                            row.put(column.getName(), RowMapperUtil.getColumnValue(rs, column));
                        }
                        batch.add(row);
                        
                        if (batch.size() >= batchSize) {
                            processBatch(insertSql, batch, columns, mapping.getTargetTable());
                            batch.clear();
                        }
                    }
                    
                    // Process remaining records
                    if (!batch.isEmpty()) {
                        processBatch(insertSql, batch, columns, mapping.getTargetTable());
                    }
                }
            }
            
            log.info("Completed migration for partition: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error migrating partition: {} -> {}", mapping.getSourceTable(), mapping.getTargetTable(), e);
            throw new MigrationException("Migration failed", e);
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

    @Override
    @Transactional
    public void truncateTable(TruncateRequest request) {
        log.info("Starting truncate operation for table: {}", request.getTargetTable());
        
        try {
            validateTableExists(request.getTargetTable());
            if (StringUtils.hasText(request.getPartitionValue())) {
                validatePartitionExists(request.getTargetTable(), request.getPartitionValue());
            }
            
            String truncateSql = SqlBuilder.buildTruncateSql(
                request.getTargetTable(), 
                request.getPartitionValue(), 
                request.isCascade()
            );
            
            targetJdbcTemplate.execute(truncateSql);
            
            String operation = StringUtils.hasText(request.getPartitionValue()) ? "partition" : "table";
            log.info("Successfully truncated {}: {}", 
                operation, 
                StringUtils.hasText(request.getPartitionValue()) ? 
                    request.getTargetTable() + "." + request.getPartitionValue() : 
                    request.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error truncating table: {}", request.getTargetTable(), e);
            throw new MigrationException("Truncate operation failed", e);
        }
    }

    @Override
    @Transactional
    public void deleteRows(DeleteRequest request) {
        log.info("Starting delete operation for table: {}", request.getTargetTable());
        
        try {
            validateTableExists(request.getTargetTable());
            if (StringUtils.hasText(request.getPartitionValue())) {
                validatePartitionExists(request.getTargetTable(), request.getPartitionValue());
            }
            
            String deleteSql = SqlBuilder.buildDeleteSql(
                request.getTargetTable(),
                request.getPartitionValue(),
                request.getWhereClause()
            );
            
            int totalDeleted = BatchProcessor.processDeleteBatch(
                deleteSql,
                Math.min(request.getBatchSize(), MigrationConstants.MAX_BATCH_SIZE),
                targetJdbcTemplate
            );
            
            String operation = StringUtils.hasText(request.getPartitionValue()) ? "partition" : "table";
            log.info("Successfully deleted {} rows from {}: {}", 
                totalDeleted,
                operation, 
                StringUtils.hasText(request.getPartitionValue()) ? 
                    request.getTargetTable() + "." + request.getPartitionValue() : 
                    request.getTargetTable());
            
        } catch (Exception e) {
            log.error("Error deleting rows from table: {}", request.getTargetTable(), e);
            throw new MigrationException("Delete operation failed", e);
        }
    }

    private void validateTableExists(String tableName) {
        List<Integer> result = targetJdbcTemplate.queryForList(
            MigrationConstants.CHECK_TABLE_EXISTS, 
            Integer.class, 
            tableName
        );
        
        if (result.isEmpty()) {
            throw new MigrationException("Table " + tableName + " does not exist in target database");
        }
    }

    private void validatePartitionExists(String tableName, String partitionValue) {
        List<Integer> result = targetJdbcTemplate.queryForList(
            MigrationConstants.CHECK_PARTITION_EXISTS,
            Integer.class,
            tableName,
            partitionValue
        );
        
        if (result.isEmpty()) {
            throw new MigrationException("Partition " + partitionValue + 
                " does not exist in table " + tableName);
        }
    }

    private List<ColumnInfo> getTableColumns(String tableName) {
        return sourceJdbcTemplate.query(
            MigrationConstants.GET_TABLE_COLUMNS,
            RowMapperUtil.COLUMN_INFO_MAPPER,
            tableName
        );
    }

    private List<ColumnInfo> getTableColumns(String tableName, List<String> columns) {
        String placeholders = String.join(",", Collections.nCopies(columns.size(), "?"));
        String sql = String.format(MigrationConstants.GET_TABLE_COLUMNS_WITH_FILTER, placeholders);
        
        List<Object> params = new ArrayList<>();
        params.add(tableName);
        params.addAll(columns);
        
        return sourceJdbcTemplate.query(sql, params.toArray(), RowMapperUtil.COLUMN_INFO_MAPPER);
    }

    private List<String> getPartitions(String tableName, String partitionKey) {
        return sourceJdbcTemplate.queryForList(
            MigrationConstants.GET_PARTITIONS,
            String.class,
            tableName,
            "%" + partitionKey + "%"
        );
    }

    private void processBatch(String insertSql, List<Map<String, Object>> batch, List<ColumnInfo> columns, String tableName) {
        String failedRecordsFile = getFailedRecordsFileName(tableName);
        BatchProcessor.processBatch(
            insertSql,
            batch,
            columns,
            tableName,
            targetJdbcTemplate,
            row -> logFailedRecord(failedRecordsFile, row, "Failed to insert record")
        );
    }
} 