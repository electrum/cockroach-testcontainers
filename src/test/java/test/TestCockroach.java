package test;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCockroach
{
    @BeforeClass(timeOut = 60_000)
    public void setup()
            throws SQLException
    {
        // run with longer timeout to allow for image download
        verifyCockroach();
    }

    @Test(timeOut = 10_000, invocationCount = 100)
    public void testCockroach()
            throws SQLException
    {
        verifyCockroach();
    }

    private void verifyCockroach()
            throws SQLException
    {
        CockroachContainer container = new CockroachContainer("cockroachdb/cockroach:v21.1.11");
        container.start();
        container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(getClass())));

        try (Connection connection = container.createConnection("");
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 42")) {
            assertTrue(rs.next());
            assertEquals(rs.getLong(1), 42L);
            assertFalse(rs.next());
        }
        finally {
            container.stop();
        }
    }
}
