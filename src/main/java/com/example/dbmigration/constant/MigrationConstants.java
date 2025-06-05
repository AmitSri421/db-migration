package com.example.dbmigration.constant;

public final class MigrationConstants {
    private MigrationConstants() {
        // Prevent instantiation
    }

    // SQL Queries
    public static final String CHECK_TABLE_EXISTS = "SELECT 1 FROM all_tables WHERE table_name = ?";
    public static final String CHECK_PARTITION_EXISTS = "SELECT 1 FROM all_tab_partitions WHERE table_name = ? AND partition_name = ?";
    public static final String GET_TABLE_COLUMNS = "SELECT column_name, data_type, data_length, data_precision, data_scale " +
            "FROM all_tab_columns WHERE table_name = ? ORDER BY column_id";
    public static final String GET_TABLE_COLUMNS_WITH_FILTER = "SELECT column_name, data_type, data_length, data_precision, data_scale " +
            "FROM all_tab_columns WHERE table_name = ? AND column_name IN (%s) ORDER BY column_id";
    public static final String GET_PARTITIONS = "SELECT partition_name FROM all_tab_partitions WHERE table_name = ? AND partition_key_column LIKE ?";

    // File Constants
    public static final String FAILED_RECORDS_HEADER = "timestamp,error_message,record_data\n";
    public static final String FAILED_RECORDS_FORMAT = "%s/%s_failed_records_%s.csv";
    public static final String TIMESTAMP_FORMAT = "yyyyMMdd_HHmmss";
    public static final String ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    // Default Values
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final int MAX_BATCH_SIZE = 10000;
} 