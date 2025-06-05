package com.example.dbmigration.model;

import lombok.Data;
import jakarta.validation.constraints.NotBlank;
import java.util.List;

@Data
public class TruncateRequest {
    @NotBlank(message = "Target table name is required")
    private String targetTable;
    
    private String partitionValue;  // Optional, for truncating specific partition
    
    private boolean cascade = false;  // Optional, for CASCADE TRUNCATE
} 