package org.pragmaticminds.crunch.chronicle.consumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.*;

/**
 * Manages Chronicle Consumers and their offsets based on a SQLITE database file.
 * <p>
 * A Single Table named "CONSUMER" is created which has colums (CONSUMER VARCHAR(50), LAST_OFFSET BIGINT).
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class JdbcConsumerManager implements ConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(JdbcConsumerManager.class);

    private static final String CONSUMER_FILE = "consumers.db";
    // Create Statement
    private static final String CREATE_STMT =
            "CREATE TABLE IF NOT EXISTS CONSUMER (CONSUMER VARCHAR(50) PRIMARY KEY UNIQUE , LAST_OFFSET BIGINT);";
    // Insert or Update Statement
    private static final String UPSERT_STMT =
            "INSERT OR REPLACE INTO CONSUMER (CONSUMER, LAST_OFFSET) VALUES (?, ?);";
    // Request offset
    private static final String GET_OFFSET =
            "SELECT LAST_OFFSET FROM CONSUMER WHERE CONSUMER = ?;";


    final transient Connection connection;
    public final long acknowledgeRate;

    // Prepared Statements
    private transient PreparedStatement upsertStatement;
    private transient PreparedStatement getOffsetStatement;



    // Ack Counter
    private long count = 0;

    /**
     * Creates a new manager.
     * Information is stored in a sqlite database named consumers.db in the given path.
     * If one exists this one is taken, otherwise a new one is created.
     *
     * Writes every 100th ack to the underlying persistent store
     *
     * @param path Path to create (or read from) the db
     */
    public JdbcConsumerManager(Path path) {
        this(path, 100);
    }

    /**
     * Creates a new manager.
     * Information is stored in a sqlite database named consumers.db in the given path.
     * If one exists this one is taken, otherwise a new one is created.
     *
     * @param path            Path to create (or read from) the db
     * @param acknowledgeRate Each n-th record is persisted to disk
     */
    public JdbcConsumerManager(Path path, long acknowledgeRate) {
        this.acknowledgeRate = acknowledgeRate;
        String filename = path.resolve(CONSUMER_FILE).toAbsolutePath().toString();
        try {
            connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", filename));
            init(connection);
        } catch (SQLException e) {
            throw new ConsumerManagerException("Not able to connect to sqlite file '" + filename + "'", e);
        }
    }

    /**
     * Create Table if not exists.
     *
     * @param conn Connection to use
     * @throws SQLException When problems occur
     */
    private void init(Connection conn) throws SQLException {
        // Create the table if it does not (yet) exist
        try (Statement statement = conn.createStatement()) {
            statement.execute(CREATE_STMT);
        }
        upsertStatement = conn.prepareStatement(UPSERT_STMT);
        getOffsetStatement = conn.prepareStatement(GET_OFFSET);
    }

    /**
     * Returns the offset associated with the given consumer
     *
     * @param consumer Name of the consumer
     * @return Offset of the consumer or -1 if no offset is stored
     */
    @Override
    public long getOffset(String consumer) {
        try {
            getOffsetStatement.setString(1, consumer);
            try (ResultSet resultSet = getOffsetStatement.executeQuery()) {
                if (!resultSet.next()) {
                    // No offset information preset, we return -1
                    return -1;
                } else {
                    return resultSet.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new ConsumerManagerException("Not able to retrieve offset.", e);
        }
    }

    /**
     * Acknowledges, i.e., stores the offset persistend
     *
     * @param consumer name of the consumer
     * @param offset   offset to store
     */
    @Override
    public void acknowledgeOffset(String consumer, long offset, boolean useAcknowledgeRate) {
        if (useAcknowledgeRate) {
            count++;
            if (count == acknowledgeRate) {
                count = 0;
                updateAcknowledgment(consumer, offset);
            }
        } else {
            updateAcknowledgment(consumer, offset);
        }
    }


    private void updateAcknowledgment(String consumer, long offset) {
        logger.debug("Committing offset {} for consumer {} to sqlite", offset, consumer);
        try {
            upsertStatement.setString(1, consumer);
            upsertStatement.setLong(2, offset);
            int i = upsertStatement.executeUpdate();
            // Should modify one line
            if (i != 1) {
                throw new ConsumerManagerException("Not able to acknowledge offset");
            }
        } catch (SQLException e) {
            throw new ConsumerManagerException("Not able to acknowledge offset", e);
        }
    }

    @Override
    @SuppressWarnings("squid:S1166") // Do not log SQL Exception
    public void close() {
        try {
            this.upsertStatement.close();
            this.connection.close();
        } catch (SQLException e) {
            // Intentionally do nothing
        }
    }

}
