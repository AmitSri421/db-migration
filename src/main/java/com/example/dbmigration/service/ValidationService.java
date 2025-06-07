package com.example.dbmigration.service;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;
import java.util.List;
import java.util.Map;

public interface ValidationService {
    /**
     * Validate a single table or partition
     * @param request The validation request
     * @return Validation result
     */
    ValidationResult validate(ValidationRequest request);
    
    /**
     * Validate multiple tables or partitions
     * @param requests List of validation requests
     * @return Map of table/partition names to validation results
     */
    Map<String, ValidationResult> validateBatch(List<ValidationRequest> requests);
    
    /**
     * Generate a validation report
     * @param results List of validation results
     * @return Path to the generated report file
     */
    String generateReport(List<ValidationResult> results);
    
    /**
     * Get validation history
     * @param tableName Optional table name filter
     * @param startDate Optional start date filter
     * @param endDate Optional end date filter
     * @return List of validation results
     */
    List<ValidationResult> getValidationHistory(String tableName, String startDate, String endDate);
} 