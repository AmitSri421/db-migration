package com.example.dbmigration.service;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import java.util.List;
import java.util.Map;

public interface ValidationService {
    /**
     * Validates a single table or partition
     * @param request The validation request containing table names and validation options
     * @return The validation result
     */
    ValidationResult validate(ValidationRequest request);

    /**
     * Validates multiple tables or partitions
     * @param requests List of validation requests
     * @return Map of validation results keyed by table name
     */
    Map<String, ValidationResult> validateBatch(List<ValidationRequest> requests);

    /**
     * Generates a validation report for the given results
     * @param results List of validation results to include in the report
     * @return Path to the generated report file
     */
    String generateReport(List<ValidationResult> results);
} 