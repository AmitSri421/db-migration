package com.example.dbmigration.service;

public interface DatabaseConnectionService {
    boolean testSourceConnection();
    boolean testTargetConnection();
} 