package com.example.dbmigration.util;

public final class MetadataQueries {
    private MetadataQueries() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    // Row count queries
    public static final String GET_ROW_COUNT = "SELECT COUNT(*) FROM %s";
    public static final String GET_PARTITION_ROW_COUNT = 
        "SELECT COUNT(*) FROM %s WHERE %s = '%s'";

    // Index queries
    public static final String GET_INDEXES = 
        "SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_NAME = ?";

    // Constraint queries
    public static final String GET_PRIMARY_KEYS = 
        "SELECT COLUMN_NAME FROM USER_CONS_COLUMNS WHERE CONSTRAINT_NAME IN " +
        "(SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? " +
        "AND CONSTRAINT_TYPE = 'P') ORDER BY POSITION";

    public static final String GET_FOREIGN_KEYS = 
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? " +
        "AND CONSTRAINT_TYPE = 'R'";

    public static final String GET_UNIQUE_KEYS = 
        "SELECT COLUMN_NAME FROM USER_CONS_COLUMNS WHERE CONSTRAINT_NAME IN " +
        "(SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? " +
        "AND CONSTRAINT_TYPE = 'U') ORDER BY POSITION";

    // Column queries
    public static final String GET_COLUMNS = 
        "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE, " +
        "DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? " +
        "ORDER BY COLUMN_ID";

    // Partition queries
    public static final String GET_PARTITIONS = 
        "SELECT partition_name, partition_position, high_value " +
        "FROM user_tab_partitions " +
        "WHERE table_name = ? " +
        "ORDER BY partition_position";

    public static final String GET_PARTITION_TYPE = 
        "SELECT PARTITIONING_TYPE, SUBPARTITIONING_TYPE FROM USER_PART_TABLES " +
        "WHERE TABLE_NAME = ?";
} 