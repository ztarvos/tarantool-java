package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.function.Executable;
import org.tarantool.TarantoolConnection;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class JdbcPreparedStatementIT extends AbstractJdbcIT {
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
    public void testSetParameter() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test_types VALUES (" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 1000);//INT
        prep.setString(2, (String)testRow[1]);//CHAR
        prep.setString(3, (String)testRow[2]);//VARCHAR
        prep.setString(4, (String)testRow[3]);//LONGVARCHAR
        prep.setBigDecimal(5, (BigDecimal)testRow[4]);//NUMERIC
        prep.setBigDecimal(6, (BigDecimal)testRow[5]);//DECIMAL
        prep.setBoolean(7, (Boolean)testRow[6]);//BIT
        prep.setByte(8, (Byte)testRow[7]);//TINYINT
        prep.setShort(9, (Short)testRow[8]);//SMALLINT
        prep.setInt(10, (Integer)testRow[9]);//INTEGER
        prep.setLong(11, (Long)testRow[10]);//BIGINT
        prep.setFloat(12, (Float)testRow[11]);//REAL
        prep.setDouble(13, (Double)testRow[12]);//FLOAT
        prep.setBytes(14, (byte[])testRow[13]);//BINARY
        prep.setBytes(15, (byte[])testRow[14]);//VARBINARY
        prep.setBytes(16, (byte[])testRow[15]);//LONGVARBINARY
        prep.setDate(17, (Date)testRow[16]);//DATE
        prep.setTime(18, (Time)testRow[17]);//TIME
        prep.setTimestamp(19, (Timestamp)testRow[18]);//TIMESTAMP

        int count = prep.executeUpdate();
        assertEquals(1, count);

        prep.close();

        prep = conn.prepareStatement("SELECT * FROM test_types WHERE f1 = ?");
        prep.setInt(1, 1000);

        ResultSet rs = prep.executeQuery();
        assertNotNull(rs);

        assertTrue(rs.next());
        assertEquals(1000, rs.getInt(1));//INT
        assertEquals(testRow[1], rs.getString(2));//CHAR
        assertEquals(testRow[2], rs.getString(3));//VARCHAR
        assertEquals(testRow[3], rs.getString(4)); //LONGVARCHAR
        assertEquals(testRow[4], rs.getBigDecimal(5));//NUMERIC
        assertEquals(testRow[5], rs.getBigDecimal(6));//DECIMAL
        assertEquals(testRow[6], rs.getBoolean(7));//BIT
        assertEquals(testRow[7], rs.getByte(8));//TINYINT
        assertEquals(testRow[8], rs.getShort(9));//SMALLINT
        assertEquals(testRow[9], rs.getInt(10));//INTEGER
        assertEquals(testRow[10], rs.getLong(11));//BIGINT
        assertEquals((Float)testRow[11], rs.getFloat(12), 1e-10f);//REAL
        assertEquals((Double)testRow[12], rs.getDouble(13), 1e-10d);//FLOAT
        //Issue#45
        //assertTrue(Arrays.equals((byte[])testRow[13], rs.getBytes(14)));//BINARY
        //assertTrue(Arrays.equals((byte[])testRow[14], rs.getBytes(15)));//VARBINARY
        //assertTrue(Arrays.equals((byte[])testRow[15], rs.getBytes(16)));//LONGVARBINARY
        assertEquals(testRow[16], rs.getDate(17));//DATE
        assertEquals(testRow[17], rs.getTime(18));//TIME
        assertEquals(testRow[18], rs.getTimestamp(19));//TIMESTAMP

        rs.close();
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
    public void testWrapper() throws SQLException {
        prep = conn.prepareStatement("TEST");
        assertFalse(prep.isWrapperFor(null));
        assertFalse(prep.isWrapperFor(TarantoolConnection.class));
        assertTrue(prep.isWrapperFor(SQLPreparedStatement.class));

        assertSame(prep, prep.unwrap(SQLPreparedStatement.class));

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                prep.unwrap(TarantoolConnection.class);
            }
        });

        assertTrue(e.getMessage().startsWith("Cannot unwrap to"));
    }
}
