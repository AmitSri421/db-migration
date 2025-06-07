package com.example.dbmigration.util;

public final class MetadataQueries {
    private MetadataQueries() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    // Row count queries
    public static final String GET_ROW_COUNT = "SELECT COUNT(*) FROM %s";
    public static final String GET_PARTITION_ROW_COUNT = "SELECT COUNT(*) FROM %s PARTITION(%s)";

    // Index queries
    public static final String GET_INDEXES = 
        "SELECT index_name, column_name, column_position " +
        "FROM user_ind_columns " +
        "WHERE table_name = ? " +
        "ORDER BY index_name, column_position";

    // Constraint queries
    public static final String GET_PRIMARY_KEYS = 
        "SELECT column_name " +
        "FROM user_cons_columns " +
        "WHERE table_name = ? " +
        "AND constraint_name IN ( " +
        "    SELECT constraint_name " +
        "    FROM user_constraints " +
        "    WHERE table_name = ? " +
        "    AND constraint_type = 'P' " +
        ") " +
        "ORDER BY position";

    public static final String GET_FOREIGN_KEYS = 
        "SELECT a.constraint_name, a.column_name, c.r_constraint_name " +
        "FROM user_cons_columns a " +
        "JOIN user_constraints c ON a.constraint_name = c.constraint_name " +
        "WHERE c.table_name = ? " +
        "AND c.constraint_type = 'R' " +
        "ORDER BY a.constraint_name, a.position";

    public static final String GET_UNIQUE_KEYS = 
        "SELECT column_name " +
        "FROM user_cons_columns " +
        "WHERE table_name = ? " +
        "AND constraint_name IN ( " +
        "    SELECT constraint_name " +
        "    FROM user_constraints " +
        "    WHERE table_name = ? " +
        "    AND constraint_type = 'U' " +
        ") " +
        "ORDER BY position";

    // Column queries
    public static final String GET_COLUMNS = 
        "SELECT column_name, data_type, data_length, data_precision, data_scale, " +
        "       nullable, data_default, column_id " +
        "FROM user_tab_columns " +
        "WHERE table_name = ? " +
        "ORDER BY column_id";

    // Partition queries
    public static final String GET_PARTITIONS = 
        "SELECT partition_name, partition_position, high_value " +
        "FROM user_tab_partitions " +
        "WHERE table_name = ? " +
        "ORDER BY partition_position";

    public static final String GET_PARTITION_TYPE = 
        "SELECT partitioning_type, subpartitioning_type " +
        "FROM user_part_tables " +
        "WHERE table_name = ?";
} 