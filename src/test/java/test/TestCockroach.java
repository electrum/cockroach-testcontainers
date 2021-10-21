package test;

import org.testcontainers.containers.CockroachContainer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCockroach
{
    static {
        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSSZ");
    }

    @BeforeClass(timeOut = 120_000)
    public void setup()
            throws SQLException
    {
        // run with longer timeout to allow for image download
        verifyCockroach(Duration.ofSeconds(90));
    }

    @Test(timeOut = 60_000, invocationCount = 100)
    public void testCockroach()
            throws SQLException
    {
        verifyCockroach(Duration.ofSeconds(30));
    }

    private static void verifyCockroach(Duration duration)
            throws SQLException
    {
        CockroachContainer container = new CockroachContainer("cockroachdb/cockroach:v21.1.11")
                .withStartupTimeout(duration)
                .withConnectTimeoutSeconds(toIntExact(duration.toSeconds()));

        container.start();

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
