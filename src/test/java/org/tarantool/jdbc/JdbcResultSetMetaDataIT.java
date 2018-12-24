package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.tarantool.TarantoolConnection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcResultSetMetaDataIT extends AbstractJdbcIT {
    @Test
    public void testColumnNames() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_types");
        assertNotNull(rs);
        assertTrue(rs.next());

        ResultSetMetaData rsMeta = rs.getMetaData();

        assertEquals(19, rsMeta.getColumnCount());

        for (int i = 1; i <= 19; i++)
            assertEquals("F" + i, rsMeta.getColumnName(i));

        rs.close();
        stmt.close();
    }

    @Test
    public void testWrapper() throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery("SELECT * FROM test");
            final ResultSetMetaData meta = rs.getMetaData();

            try {
                assertFalse(meta.isWrapperFor(null));
                assertFalse(meta.isWrapperFor(TarantoolConnection.class));
                assertTrue(meta.isWrapperFor(SQLResultSetMetaData.class));

                assertSame(meta, meta.unwrap(SQLResultSetMetaData.class));

                SQLException e = assertThrows(SQLException.class, new Executable() {
                    @Override
                    public void execute() throws Throwable {
                        meta.unwrap(TarantoolConnection.class);
                    }
                });

                assertTrue(e.getMessage().startsWith("Cannot unwrap to"));

            } finally {
                rs.close();
            }
        } finally {
            stmt.close();
        }
    }
}
