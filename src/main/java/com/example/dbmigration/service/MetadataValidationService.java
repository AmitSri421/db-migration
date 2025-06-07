package com.example.dbmigration.service;

import com.example.dbmigration.model.ValidationRequest;
import com.example.dbmigration.model.ValidationResult;

public interface MetadataValidationService {
    /**
     * Validates metadata between source and target tables
     * @param request The validation request containing table names and validation types
     * @return ValidationResult containing the results of all requested validations
     */
    ValidationResult validateMetadata(ValidationRequest request);
} 