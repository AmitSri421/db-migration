package com.example.dbmigration.model;

import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
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