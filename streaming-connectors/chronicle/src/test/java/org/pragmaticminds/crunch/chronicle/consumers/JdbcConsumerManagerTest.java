package org.pragmaticminds.crunch.chronicle.consumers;

import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * @author julian
 * Created by julian on 16.08.18
 */
public class JdbcConsumerManagerTest {

    public static final String CONSUMER = "Julian";

    @Test
    public void initialize() throws SQLException {
        JdbcConsumerManager consumer = createJdbcConsumerManager();

        // Assert connection exists
        assertFalse(consumer.connection.isClosed());
        // Assert that the Table exists
        ResultSet resultSet;
        try (Statement statement = consumer.connection.createStatement()) {
            resultSet = statement.executeQuery("SELECT * FROM CONSUMER");
        }
        // Not null means the query did not fail
        assertNotNull(resultSet);

        consumer.close();
    }

    @Test
    public void storeConsumer() throws SQLException {
        JdbcConsumerManager manager = createJdbcConsumerManager();

                //call 1000 time cause only every 1000 times (default value) offset will be persisted due to performance issues
        for(int iterations=0;iterations<1000;iterations++){
            manager.acknowledgeOffset(CONSUMER, 123456789);
        }

        // Check in the DB
        try (ResultSet resultSet = manager.connection.createStatement()
                .executeQuery("SELECT CONSUMER, LAST_OFFSET FROM CONSUMER WHERE CONSUMER = 'Julian'")) {

            assertTrue(resultSet.next());
            assertEquals(123456789, resultSet.getLong(2));
        }

        manager.close();
    }

    @Test
    public void getOffset_nothingPresent_returnsMinusOne() {
        JdbcConsumerManager manager = createJdbcConsumerManager();

        long offset = manager.getOffset("Julian");

        manager.close();

        assertEquals(-1, offset);
    }

    @Test
    public void getOffset_offsetPresent_isReturned() {
        JdbcConsumerManager manager = createJdbcConsumerManager();

        //call 1000 time cause only every 1000 times (default value) offset will be persisted due to performance issues
        for(int iterations=0;iterations<1000;iterations++){
            manager.acknowledgeOffset("Julian", 123456789);
        }

        long offset = manager.getOffset("Julian");

        manager.close();

        assertEquals(123456789, offset);
    }

    /**
     * 100.000 acknowledges should not take longer than 1s
     *
     * Runtimes can vary. Thus is ignored.
     */
    @Test
    @Ignore // Runtimes can vary on Build server thus sometimes fails. Thus is ignored.
    public void checkPerformance() {
        JdbcConsumerManager manager = createJdbcConsumerManager();

        long start = System.currentTimeMillis();
        for (int i = 1; i <= 100; i++) {
            manager.acknowledgeOffset("Julian", i);
        }
        long stop = System.currentTimeMillis();

        long durationMs = stop - start;

        assertTrue("Duration was too long, was " + durationMs, durationMs < 1_000);
    }

    @NotNull
    private JdbcConsumerManager createJdbcConsumerManager() {
        try {
            Path basePath = Paths.get(System.getProperty("java.io.tmpdir"));
            Path tmp = Files.createTempDirectory(basePath, "jdbc-consumer-");
            return new JdbcConsumerManager(tmp);
        } catch (IOException e) {
            throw new RuntimeException("");
        }
    }
}