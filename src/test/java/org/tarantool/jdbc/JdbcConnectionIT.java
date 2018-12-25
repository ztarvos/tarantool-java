package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.tarantool.TarantoolConnection;

import java.lang.reflect.Field;
import java.net.Socket;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("Since15")
public class JdbcConnectionIT extends AbstractJdbcIT {
    @Test
    public void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        stmt.close();
    }

    @Test
    public void testPrepareStatement() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES(?, ?)");
        assertNotNull(prep);
        prep.close();
    }

    @Test
    public void testCloseIsClosed() throws SQLException {
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        conn.close();
    }

    @Test
    public void testGetMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        assertNotNull(meta);
    }

    @Test
    public void testGetSetNetworkTimeout() throws Exception {
        assertEquals(0, conn.getNetworkTimeout());

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                conn.setNetworkTimeout(null, -1);
            }
        });
        assertEquals("Network timeout cannot be negative.", e.getMessage());

        conn.setNetworkTimeout(null, 3000);

        assertEquals(3000, conn.getNetworkTimeout());

        // Check that timeout gets propagated to the socket.
        Field tntCon = SQLConnection.class.getDeclaredField("connection");
        tntCon.setAccessible(true);

        Field sock = TarantoolConnection.class.getDeclaredField("socket");
        sock.setAccessible(true);

        assertEquals(3000, ((Socket)sock.get(tntCon.get(conn))).getSoTimeout());
    }

    @Test
    public void testClosedConnection() throws SQLException {
        conn.close();

        int i = 0;
        for (; i < 5; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                        case 0: conn.createStatement();
                            break;
                        case 1: conn.prepareStatement("TEST");
                            break;
                        case 2: conn.getMetaData();
                            break;
                        case 3: conn.getNetworkTimeout();
                            break;
                        case 4: conn.setNetworkTimeout(null, 1000);
                            break;
                        default:
                            fail();
                    }
                }
            });
            assertEquals("Connection is closed.", e.getMessage());
        }
        assertEquals(5, i);
    }

    @Test
    public void testWrapper() throws SQLException {
        assertFalse(conn.isWrapperFor(null));
        assertFalse(conn.isWrapperFor(TarantoolConnection.class));
        assertTrue(conn.isWrapperFor(SQLConnection.class));

        assertSame(conn, conn.unwrap(SQLConnection.class));

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                conn.unwrap(TarantoolConnection.class);
            }
        });

        assertTrue(e.getMessage().startsWith("Cannot unwrap to"));
    }
}