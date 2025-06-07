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
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/validation")
@RequiredArgsConstructor
public class ValidationController {

    private final ValidationService validationService;

    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validate(@RequestBody ValidationRequest request) {
        log.info("Received validation request for tables: {} -> {}", 
            request.getSourceTable(), request.getTargetTable());
        
        ValidationResult result = validationService.validate(request);
        String reportPath = validationService.generateReport(List.of(result));
        
        Map<String, Object> response = Map.of(
            "result", result,
            "reportPath", reportPath
        );
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/validate/batch")
    public ResponseEntity<Map<String, Object>> validateBatch(@RequestBody List<ValidationRequest> requests) {
        log.info("Received batch validation request for {} tables", requests.size());
        
        Map<String, ValidationResult> results = validationService.validateBatch(requests);
        String reportPath = validationService.generateReport(results.values().stream().collect(Collectors.toList()));
        
        Map<String, Object> response = Map.of(
            "results", results,
            "reportPath", reportPath
        );
        
        return ResponseEntity.ok(response);
    }
} 