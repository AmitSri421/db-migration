package com.example.dbmigration.config;

import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class MappingConfig {

    private List<TableMapping> tables = new ArrayList<>();
    private List<PartitionMapping> partitions = new ArrayList<>();

    public void setMappings(List<TableMapping> tables, List<PartitionMapping> partitions) {
        this.tables = tables;
        this.partitions = partitions;
    }

    public List<TableMapping> getTables() {
        return tables;
    }

    public List<PartitionMapping> getPartitions() {
        return partitions;
    }
} 