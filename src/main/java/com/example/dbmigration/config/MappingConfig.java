package com.example.dbmigration.config;

import com.example.dbmigration.model.PartitionMapping;
import com.example.dbmigration.model.TableMapping;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Component
public class MappingConfig {

    private final ObjectMapper objectMapper;
    private List<TableMapping> tables;
    private List<PartitionMapping> partitions;

    public MappingConfig(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void loadConfig() throws IOException {
        ClassPathResource resource = new ClassPathResource("table-mapping.json");
        MappingWrapper wrapper = objectMapper.readValue(resource.getInputStream(), MappingWrapper.class);
        this.tables = wrapper.getTables();
        this.partitions = wrapper.getPartitions();
    }

    public List<TableMapping> getTables() {
        return tables;
    }

    public List<PartitionMapping> getPartitions() {
        return partitions;
    }

    @Data
    private static class MappingWrapper {
        private List<TableMapping> tables;
        private List<PartitionMapping> partitions;
    }
} 