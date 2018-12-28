package org.tarantool.jdbc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.function.Executable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class JdbcPreparedStatementIT extends JdbcTypesIT {
    private PreparedStatement prep;

    @AfterEach
    public void tearDown() throws SQLException {
        if (prep != null && !prep.isClosed())
            prep.close();
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        assertNotNull(prep);

        prep.setInt(1, 1);
        ResultSet rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        // Reuse the prepared statement.
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("two", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 100);
        prep.setString(2, "hundred");
        int count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("hundred", getRow("test", 100).get(1));

        // Reuse the prepared statement.
        prep.setInt(1, 1000);
        prep.setString(2, "thousand");
        count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("thousand", getRow("test", 1000).get(1));
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        assertNotNull(prep);
        prep.setInt(1, 1);

        assertTrue(prep.execute());
        assertEquals(-1, prep.getUpdateCount());

        ResultSet rs = prep.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?), (?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 10);
        prep.setString(2, "ten");
        prep.setInt(3, 20);
        prep.setString(4, "twenty");

        assertFalse(prep.execute());
        assertNull(prep.getResultSet());
        assertEquals(2, prep.getUpdateCount());

        assertEquals("ten", getRow("test", 10).get(1));
        assertEquals("twenty", getRow("test", 20).get(1));
    }

    @Test void testForbiddenMethods() throws SQLException {
        prep = conn.prepareStatement("TEST");

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                        case 0: prep.executeQuery("TEST");
                            break;
                        case 1: prep.executeUpdate("TEST");
                            break;
                        case 2: prep.execute("TEST");
                            break;
                        default:
                            fail();
                    }
                }
            });
            assertEquals("The method cannot be called on a PreparedStatement.", e.getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testClosedConnection() throws SQLException {
        prep = conn.prepareStatement("TEST");

        conn.close();

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                        case 0: prep.executeQuery();
                            break;
                        case 1: prep.executeUpdate();
                            break;
                        case 2: prep.execute();
                            break;
                        default:
                            fail();
                    }
                }
            });
            assertEquals("Connection is closed.", e.getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testSetByte() throws SQLException {
        makeHelper(Byte.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(BYTE_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetInt() throws SQLException {
        makeHelper(Integer.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(INT_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetLong() throws SQLException {
        makeHelper(Long.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(LONG_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetString() throws SQLException {
        makeHelper(String.class)
        .setColumns(TntSqlType.CHAR, TntSqlType.VARCHAR, TntSqlType.TEXT)
        .setValues(STRING_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetFloat() throws SQLException {
        makeHelper(Float.class)
        .setColumns(TntSqlType.REAL)
        .setValues(FLOAT_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetDouble() throws SQLException {
        makeHelper(Double.class)
        .setColumns(TntSqlType.FLOAT, TntSqlType.DOUBLE)
        .setValues(DOUBLE_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetBigDecimal() throws SQLException {
        makeHelper(BigDecimal.class)
        .setColumns(TntSqlType.DECIMAL, TntSqlType.DECIMAL_PREC, TntSqlType.DECIMAL_PREC_SCALE,
            TntSqlType.NUMERIC, TntSqlType.NUMERIC_PREC, TntSqlType.NUMERIC_PREC_SCALE,
            TntSqlType.NUM, TntSqlType.NUM_PREC, TntSqlType.NUM_PREC_SCALE)
        .setValues(BIGDEC_VALS)
        .testSetParameter();
    }

    @Disabled("Issue#45. Binary string is reported back as char string by tarantool")
    @Test
    public void testSetByteArray() throws SQLException {
        makeHelper(byte[].class)
        .setColumns(TntSqlType.BLOB)
        .setValues(BINARY_VALS)
        .testSetParameter();
    }

    @Test
    public void testSetDate() throws SQLException {
        makeHelper(Date.class)
        .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
        .setValues(DATE_VALS)
        .testSetParameter();
    }

    @Test
    public void testExecuteReturnsAutoGeneratedIds() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_autoid(val) VALUES (?), (?), (?)", Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        assertFalse(prep.execute());
        assertEquals(3, prep.getUpdateCount());
        ResultSet rs = prep.getGeneratedKeys();
        assertNotNull(rs);
        assertTrue(rs.next());
        int a = rs.getInt(1);
        assertTrue(rs.next());
        int b = rs.getInt(1);
        assertTrue(b > a);
        assertTrue(rs.next());
        a = rs.getInt(1);
        assertTrue(a > b);
    }

    @Test
    public void testExecuteUpdateReturnsAutoGeneratedIds() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_autoid(val) VALUES (?), (?), (?)", Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        assertEquals(3, prep.executeUpdate());
        ResultSet rs = prep.getGeneratedKeys();
        assertNotNull(rs);
        assertTrue(rs.next());
        int a = rs.getInt(1);
        assertTrue(rs.next());
        int b = rs.getInt(1);
        assertTrue(b > a);
        assertTrue(rs.next());
        a = rs.getInt(1);
        assertTrue(a > b);
    }

    @Test
    public void testExecuteIgnoresAutoGeneratedIds() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_autoid(val) VALUES (?), (?), (?)", Statement.NO_GENERATED_KEYS);
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        assertFalse(prep.execute());
        assertEquals(3, prep.getUpdateCount());
        ResultSet rs = prep.getGeneratedKeys();
        assertNotNull(rs);
        assertFalse(rs.next());
    }

    @Test
    public void testExecuteUpdateIgnoresAutoGeneratedIds() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_autoid(val) VALUES (?), (?), (?)", Statement.NO_GENERATED_KEYS);
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        assertEquals(3, prep.executeUpdate());
        ResultSet rs = prep.getGeneratedKeys();
        assertNotNull(rs);
        assertFalse(rs.next());
    }

    @Test
    public void testExecuteIgnoresAutoGeneratedIds2() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_autoid(val) VALUES (?), (?), (?)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);

        assertFalse(prep.execute());
        assertEquals(3, prep.getUpdateCount());
        ResultSet rs = prep.getGeneratedKeys();
        assertNotNull(rs);
        assertFalse(rs.next());
    }

    @Test
    public void testExecuteUpdateIgnoresAutoGeneratedIds2() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_autoid(val) VALUES (?), (?), (?)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        assertEquals(3, prep.executeUpdate());
        ResultSet rs = prep.getGeneratedKeys();
        assertNotNull(rs);
        assertFalse(rs.next());
    }
}
