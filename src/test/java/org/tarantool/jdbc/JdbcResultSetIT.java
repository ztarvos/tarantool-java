package org.tarantool.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcResultSetIT extends JdbcTypesIT {
    private Statement stmt;

    @BeforeEach
    public void setUp() throws Exception {
        stmt = conn.createStatement();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();
    }

    @Test
    public void testEmpty() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(rs);
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testIteration() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM test WHERE id IN (1,2,3) ORDER BY id");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testGetByteColumn() throws SQLException {
        makeHelper(Byte.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(BYTE_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetShortColumn() throws SQLException {
        makeHelper(Short.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(SHORT_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetIntColumn() throws SQLException {
        makeHelper(Integer.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(INT_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetLongColumn() throws SQLException {
        makeHelper(Long.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(LONG_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetBigDecimalColumn() throws SQLException {
        makeHelper(BigDecimal.class)
        .setColumns(TntSqlType.DECIMAL, TntSqlType.DECIMAL_PREC, TntSqlType.DECIMAL_PREC_SCALE,
            TntSqlType.NUMERIC, TntSqlType.NUMERIC_PREC, TntSqlType.NUMERIC_PREC_SCALE,
            TntSqlType.NUM, TntSqlType.NUM_PREC, TntSqlType.NUM_PREC_SCALE)
        .setValues(BIGDEC_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetFloatColumn() throws SQLException {
        makeHelper(Float.class)
        .setColumns(TntSqlType.REAL)
        .setValues(FLOAT_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetDoubleColumn() throws SQLException {
        makeHelper(Double.class)
        .setColumns(TntSqlType.FLOAT, TntSqlType.DOUBLE)
        .setValues(DOUBLE_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetStringColumn() throws SQLException {
        makeHelper(String.class)
        .setColumns(TntSqlType.CHAR, TntSqlType.VARCHAR, TntSqlType.TEXT)
        .setValues(STRING_VALS)
        .testGetColumn();
    }

    @Test
    public void testGetByteArrayColumn() throws SQLException {
        makeHelper(byte[].class)
        .setColumns(TntSqlType.BLOB)
        .setValues(BINARY_VALS)
        .testGetColumn();
    }
}
