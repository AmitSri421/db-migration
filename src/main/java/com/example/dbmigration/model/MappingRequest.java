package com.example.dbmigration.model;

import lombok.Data;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

@Data
public class MappingRequest {
    @NotNull(message = "Tables list cannot be null")
    @Valid
    private List<TableMapping> tables;
    
    @NotNull(message = "Partitions list cannot be null")
    @Valid
    private List<PartitionMapping> partitions;
} 