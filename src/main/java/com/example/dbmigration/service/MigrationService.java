package com.example.dbmigration.service;

import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;
import com.example.dbmigration.model.TruncateRequest;
import com.example.dbmigration.model.DeleteRequest;

public interface MigrationService {
    void migrateTable(TableMapping mapping);
    void migratePartition(PartitionMapping mapping);
    void migrateAllTables();
    void migrateAllPartitions();
    void migrateAll();
    void truncateTable(TruncateRequest request);
    void deleteRows(DeleteRequest request);
} 