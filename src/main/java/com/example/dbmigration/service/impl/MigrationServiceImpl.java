package com.example.dbmigration.service.impl;

import com.example.dbmigration.config.MappingConfig;
import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;
import com.example.dbmigration.service.MigrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

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
            // Get column information
            List<String> columns = getTableColumns(mapping.getSourceTable());
            
            // Build SQL statements
            String selectSql = buildSelectSql(mapping.getSourceTable(), columns, mapping.getWhereClause());
            String insertSql = buildInsertSql(mapping.getTargetTable(), columns);
            
            // Process in batches
            int batchSize = mapping.getBatchSize();
            List<Map<String, Object>> batch = new ArrayList<>();
            
            sourceJdbcTemplate.query(selectSql, rs -> {
                Map<String, Object> row = new java.util.HashMap<>();
                for (String column : columns) {
                    row.put(column, rs.getObject(column));
                }
                batch.add(row);
                
                if (batch.size() >= batchSize) {
                    processBatch(insertSql, batch, columns);
                    batch.clear();
                }
            });
            
            // Process remaining records
            if (!batch.isEmpty()) {
                processBatch(insertSql, batch, columns);
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
                
                // Get column information
                List<String> columns = getTableColumns(mapping.getSourceTable());
                
                // Build SQL statements with partition
                String selectSql = buildPartitionSelectSql(mapping.getSourceTable(), columns, 
                    mapping.getPartitionKey(), partition, mapping.getWhereClause());
                String insertSql = buildInsertSql(mapping.getTargetTable(), columns);
                
                // Process in batches
                int batchSize = mapping.getBatchSize();
                List<Map<String, Object>> batch = new ArrayList<>();
                
                sourceJdbcTemplate.query(selectSql, rs -> {
                    Map<String, Object> row = new java.util.HashMap<>();
                    for (String column : columns) {
                        row.put(column, rs.getObject(column));
                    }
                    batch.add(row);
                    
                    if (batch.size() >= batchSize) {
                        processBatch(insertSql, batch, columns);
                        batch.clear();
                    }
                });
                
                // Process remaining records
                if (!batch.isEmpty()) {
                    processBatch(insertSql, batch, columns);
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

    private List<String> getTableColumns(String tableName) {
        String sql = "SELECT column_name FROM all_tab_columns WHERE table_name = ? ORDER BY column_id";
        return sourceJdbcTemplate.queryForList(sql, String.class, tableName);
    }

    private List<String> getPartitions(String tableName, String partitionKey) {
        String sql = "SELECT partition_name FROM all_tab_partitions WHERE table_name = ? AND partition_key_column LIKE ?";
        return sourceJdbcTemplate.queryForList(sql, String.class, tableName, "%" + partitionKey + "%");
    }

    private String buildSelectSql(String tableName, List<String> columns, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columns));
        sql.append(" FROM ").append(tableName);
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }

    private String buildPartitionSelectSql(String tableName, List<String> columns, 
            String partitionKey, String partition, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columns));
        sql.append(" FROM ").append(tableName);
        sql.append(" PARTITION(").append(partition).append(")");
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }

    private String buildInsertSql(String tableName, List<String> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (").append(String.join(", ", columns)).append(") ");
        sql.append("VALUES (");
        sql.append("?, ".repeat(columns.size() - 1)).append("?)");
        return sql.toString();
    }

    private void processBatch(String insertSql, List<Map<String, Object>> batch, List<String> columns) {
        targetJdbcTemplate.batchUpdate(insertSql, batch, batch.size(), (ps, row) -> {
            int i = 1;
            for (String column : columns) {
                ps.setObject(i++, row.get(column));
            }
        });
    }
} 