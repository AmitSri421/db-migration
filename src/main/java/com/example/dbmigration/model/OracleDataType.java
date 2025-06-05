package com.example.dbmigration.model;

import java.sql.Types;

public enum OracleDataType {
    NUMBER("NUMBER", Types.NUMERIC),
    VARCHAR2("VARCHAR2", Types.VARCHAR),
    TIMESTAMP("TIMESTAMP(6)", Types.TIMESTAMP),
    DATE("DATE", Types.DATE),
    BLOB("BLOB", Types.BLOB),
    CLOB("CLOB", Types.CLOB),
    OTHER("OTHER", Types.OTHER);

    private final String oracleType;
    private final int jdbcType;

    OracleDataType(String oracleType, int jdbcType) {
        this.oracleType = oracleType;
        this.jdbcType = jdbcType;
    }

    public String getOracleType() {
        return oracleType;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public static OracleDataType fromString(String type) {
        for (OracleDataType dataType : values()) {
            if (dataType.oracleType.equals(type)) {
                return dataType;
            }
        }
        return OTHER;
    }
} 