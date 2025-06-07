package com.example.dbmigration.controller;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import com.example.dbmigration.service.ValidationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/validation")
@RequiredArgsConstructor
public class ValidationController {

    private final ValidationService validationService;

    @PostMapping("/validate")
    public ResponseEntity<ValidationResult> validate(@RequestBody ValidationRequest request) {
        log.info("Received validation request for tables: {} -> {}", 
            request.getSourceTable(), request.getTargetTable());
        ValidationResult result = validationService.validate(request);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/validate/batch")
    public ResponseEntity<Map<String, ValidationResult>> validateBatch(@RequestBody List<ValidationRequest> requests) {
        log.info("Received batch validation request for {} tables", requests.size());
        Map<String, ValidationResult> results = validationService.validateBatch(requests);
        return ResponseEntity.ok(results);
    }

    @PostMapping("/validate/report")
    public ResponseEntity<String> generateReport(@RequestBody List<ValidationResult> results) {
        log.info("Generating validation report for {} results", results.size());
        String reportPath = validationService.generateReport(results);
        return ResponseEntity.ok(reportPath);
    }
} 