package com.example.dbmigration.service;

import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;

public interface MigrationService {
    void migrateTable(TableMapping mapping);
    void migratePartition(PartitionMapping mapping);
    void migrateAllTables();
    void migrateAllPartitions();
    void migrateAll();
} 