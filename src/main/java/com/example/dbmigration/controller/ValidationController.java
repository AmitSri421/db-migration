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

@RestController
@RequestMapping("/api/v1/validation")
@RequiredArgsConstructor
@Slf4j
public class ValidationController {
    private final ValidationService validationService;

    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validate(@RequestBody ValidationRequest request) {
        log.info("Received validation request for table: {} -> {}", request.getSourceTable(), request.getTargetTable());
        ValidationResult result = validationService.validate(request);
        String reportPath = validationService.generateReport(List.of(result));
        return ResponseEntity.ok(Map.of(
            "validationResult", result,
            "reportPath", reportPath
        ));
    }

    @PostMapping("/validate/batch")
    public ResponseEntity<Map<String, Object>> validateBatch(@RequestBody List<ValidationRequest> requests) {
        log.info("Received batch validation request for {} tables/partitions", requests.size());
        Map<String, ValidationResult> results = validationService.validateBatch(requests);
        String reportPath = validationService.generateReport(results.values().stream().collect(Collectors.toList()));
        return ResponseEntity.ok(Map.of(
            "validationResults", results,
            "reportPath", reportPath
        ));
    }

    @GetMapping("/history")
    public ResponseEntity<List<ValidationResult>> getValidationHistory(
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {
        log.info("Retrieving validation history for table: {}, from: {} to: {}", tableName, startDate, endDate);
        List<ValidationResult> history = validationService.getValidationHistory(tableName, startDate, endDate);
        return ResponseEntity.ok(history);
    }
} 