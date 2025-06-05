package com.example.dbmigration.util;

import com.example.dbmigration.model.ColumnInfo;
import com.example.dbmigration.model.OracleDataType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

@Slf4j
public class RowMapperUtil {
    
    public static final RowMapper<ColumnInfo> COLUMN_INFO_MAPPER = (rs, rowNum) -> {
        ColumnInfo column = new ColumnInfo();
        column.setName(rs.getString("column_name"));
        column.setDataType(rs.getString("data_type"));
        column.setLength(rs.getInt("data_length"));
        column.setPrecision(rs.getInt("data_precision"));
        column.setScale(rs.getInt("data_scale"));
        return column;
    };

    public static Object getColumnValue(ResultSet rs, ColumnInfo column) throws SQLException {
        String columnName = column.getName();
        
        switch (column.getDataType()) {
            case NUMBER:
                return column.getScale() > 0 ? 
                    rs.getBigDecimal(columnName) : 
                    rs.getLong(columnName);
            case VARCHAR2:
                return rs.getString(columnName);
            case TIMESTAMP:
                return rs.getTimestamp(columnName);
            case DATE:
                return rs.getDate(columnName);
            case BLOB:
                var blob = rs.getBlob(columnName);
                return blob != null ? blob.getBytes(1, (int) blob.length()) : null;
            case CLOB:
                var clob = rs.getClob(columnName);
                return clob != null ? clob.getSubString(1, (int) clob.length()) : null;
            default:
                return rs.getObject(columnName);
        }
    }

    public static int getSqlType(OracleDataType dataType) {
        return dataType.getJdbcType();
    }

    public static void setParameterValue(java.sql.PreparedStatement ps, int index, Object value, ColumnInfo column) throws SQLException {
        switch (column.getDataType()) {
            case NUMBER:
                if (value instanceof Number) {
                    ps.setObject(index, value);
                } else {
                    ps.setNull(index, Types.NUMERIC);
                }
                break;
            case VARCHAR2:
                ps.setString(index, (String) value);
                break;
            case TIMESTAMP:
                ps.setTimestamp(index, (java.sql.Timestamp) value);
                break;
            case DATE:
                ps.setDate(index, (java.sql.Date) value);
                break;
            case BLOB:
                byte[] blobData = (byte[]) value;
                if (blobData != null) {
                    ps.setBytes(index, blobData);
                } else {
                    ps.setNull(index, Types.BLOB);
                }
                break;
            case CLOB:
                String clobData = (String) value;
                if (clobData != null) {
                    ps.setString(index, clobData);
                } else {
                    ps.setNull(index, Types.CLOB);
                }
                break;
            default:
                ps.setObject(index, value);
                break;
        }
    }
} 