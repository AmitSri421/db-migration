package com.example.dbmigration.controller;

import com.example.dbmigration.service.DatabaseConnectionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/database")
@Tag(name = "DB Connection API", description = "API for assessing database connections")
public class DatabaseConnectionController {

    private final DatabaseConnectionService databaseConnectionService;

    public DatabaseConnectionController(DatabaseConnectionService databaseConnectionService) {
        this.databaseConnectionService = databaseConnectionService;
    }

    @GetMapping("/test-connections")
    @Operation(summary = "Test All Connections", description = "Assess source and target DB connection")
    public ResponseEntity<Map<String, Boolean>> testConnections() {
        Map<String, Boolean> result = new HashMap<>();
        result.put("sourceConnection", databaseConnectionService.testSourceConnection());
        result.put("targetConnection", databaseConnectionService.testTargetConnection());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/test-source")
    @Operation(summary = "Test Source Connection", description = "Assess source DB connection")
    public ResponseEntity<Boolean> testSourceConnection() {
        return ResponseEntity.ok(databaseConnectionService.testSourceConnection());
    }

    @GetMapping("/test-target")
    @Operation(summary = "Test Target Connection", description = "Assess target DB connection")
    public ResponseEntity<Boolean> testTargetConnection() {
        return ResponseEntity.ok(databaseConnectionService.testTargetConnection());
    }
} 