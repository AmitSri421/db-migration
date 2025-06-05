package com.example.dbmigration.controller;

import com.example.dbmigration.config.MappingConfig;
import com.example.dbmigration.model.MappingRequest;
import com.example.dbmigration.model.TruncateRequest;
import com.example.dbmigration.model.DeleteRequest;
import com.example.dbmigration.service.MigrationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/migration")
@Tag(name = "Migration API", description = "API for database migration operations")
public class MigrationController {

    private final MigrationService migrationService;
    private final MappingConfig mappingConfig;

    public MigrationController(MigrationService migrationService, MappingConfig mappingConfig) {
        this.migrationService = migrationService;
        this.mappingConfig = mappingConfig;
    }

    @PostMapping("/config")
    @Operation(summary = "Update migration configuration", description = "Set the tables and partitions to be migrated")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Configuration updated successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request format or data", 
                content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, String>> updateConfig(@Valid @RequestBody MappingRequest request) {
        mappingConfig.setMappings(request.getTables(), request.getPartitions());
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Migration configuration updated");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/tables")
    @Operation(summary = "Migrate all tables", description = "Start migration for all configured tables")
    public ResponseEntity<Map<String, String>> migrateTables() {
        migrationService.migrateAllTables();
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Tables migration completed");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/partitions")
    @Operation(summary = "Migrate all partitions", description = "Start migration for all configured partitions")
    public ResponseEntity<Map<String, String>> migratePartitions() {
        migrationService.migrateAllPartitions();
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Partitions migration completed");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/all")
    @Operation(summary = "Migrate all data", description = "Start migration for all configured tables and partitions")
    public ResponseEntity<Map<String, String>> migrateAll() {
        migrationService.migrateAll();
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "All migrations completed");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/truncate")
    @Operation(summary = "Truncate target table", description = "Truncate a table in the target database")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Table truncated successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request format or data"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, String>> truncateTable(@Valid @RequestBody TruncateRequest request) {
        try {
            migrationService.truncateTable(request);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Table " + request.getTargetTable() + " truncated successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Truncate operation failed", e);
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Truncate operation failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/delete")
    @Operation(summary = "Delete rows from target table", description = "Delete rows from a table in the target database based on WHERE clause")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Rows deleted successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request format or data"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, String>> deleteRows(@Valid @RequestBody DeleteRequest request) {
        try {
            migrationService.deleteRows(request);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Rows deleted successfully from table " + request.getTargetTable());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Delete operation failed", e);
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Delete operation failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
} 