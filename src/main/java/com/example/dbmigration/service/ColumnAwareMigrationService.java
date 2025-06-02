package com.example.dbmigration.service;

import com.example.dbmigration.model.MigrationRequest;

public interface ColumnAwareMigrationService {
    void migrate(MigrationRequest request);
} 