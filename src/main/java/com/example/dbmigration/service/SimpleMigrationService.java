package com.example.dbmigration.service;

import com.example.dbmigration.model.SimpleMigrationRequest;

public interface SimpleMigrationService {
    void migrate(SimpleMigrationRequest request);
} 