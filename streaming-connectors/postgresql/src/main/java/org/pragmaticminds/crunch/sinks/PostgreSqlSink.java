package org.pragmaticminds.crunch.sinks;

import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.execution.EventSink;
import org.pragmaticminds.crunch.serialization.JsonDeserializer;
import org.pragmaticminds.crunch.serialization.JsonSerializer;
import org.pragmaticminds.crunch.sinks.exceptions.UncheckedSQLException;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A Sink for persisting of {@link Event} {@link List} in a Events table on a PostgreSQL database.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 11.09.2018
 */
public class PostgreSqlSink implements EventSink, Serializable, AutoCloseable {
    private static final String SQL_INSERT_QUERY =
        "INSERT INTO events (timestamp, type, source, parameters, raw_data) VALUES (?, ?, ?, ?::JSONB, ?::JSONB)";
    
    private static final String SQL_CHECK_IF_TABLE_AVAILABLE_QUERY =
        "SELECT EXISTS (\n"
        + "   SELECT 1\n"
        + "   FROM pg_tables \n"
        + "   WHERE schemaname = 'public'\n"
        + "   AND tablename = 'events'\n"
        + ");";
    
    private static final String SQL_CREATE_EVENTS_TABLE_QUERY =
        "CREATE TABLE Events (\n"
        + "  ID         BIGSERIAL NOT NULL PRIMARY KEY,\n"
        + "  TIMESTAMP  TIMESTAMP,\n"
        + "  TYPE       VARCHAR(255),\n"
        + "  SOURCE     VARCHAR(255),\n"
        + "  PARAMETERS JSONB,\n" // all parameters as JSON
        + "  RAW_DATA   JSONB\n" // this field saves the whole Event as a JSON
        + ")";
    
    private final transient Connection                             connection;
    private final           JsonDeserializer<Event>                deserializer;
    private final transient JsonSerializer<HashMap<String, Value>> parametersSerializer;
    private final transient JsonSerializer<Event>                  serializer;
    
    /**
     * Main constructor, builds up the connection to the PostgreSql database.
     * @param url of the database
     * @param user of the database
     * @param password of the user of the database
     */
    public PostgreSqlSink(String url, String user, String password) {
        // build up the connection to the database
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException ex) {
            throw new UncheckedSQLException("could not establish PostgreSQL connection!", ex);
        }
        
        // create the helpers
        deserializer = new JsonDeserializer<>(Event.class);
        parametersSerializer = new JsonSerializer<>();
        serializer = new JsonSerializer<>();
    
        // create database structures if not jet present
        if(!tableExists()){
            createTable();
        }
    }
    
    /**
     * Creates the Events table on the database.
     */
    private void createTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(SQL_CREATE_EVENTS_TABLE_QUERY);
        } catch (SQLException ex) {
            throw new UncheckedSQLException("could not create EVENTS table on postgres database!", ex);
        }
    }
    
    /**
     * Checks if the Events table exists on the connected database.
     *
     * @return if Events table exist, otherwise false.
     */
    private boolean tableExists() {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(SQL_CHECK_IF_TABLE_AVAILABLE_QUERY)) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        } catch (SQLException ex) {
            throw new UncheckedSQLException("could not check if table is available!", ex);
        }
    }
    
    /**
     * Executes command query on the database, which does not return a result like a Insert command.
     * !!! This method is for tests only and that is why it is package private !!!
     *
     * @param query to be executed on the database.
     */
    void executeCommand(String query){
        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
        } catch (SQLException ex) {
            throw new UncheckedSQLException(String.format("could not execute query: %s", query), ex);
        }
    }
    
    /**
     * Runs the given query on the database connected and gets results in {@link Event} type as a {@link List}.
     *
     * @param query to be executed (Only select methods and similar, no create or insert and so on).
     * @return a {@link List} of {@link Event} results from the executed query.
     */
    public List<Event> executeQuery(String query){
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(query)) {
                List<Event> results = new ArrayList<>();
                while(resultSet.next()){
                    String json = resultSet.getString(6);
                    Event event = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
                    results.add(event);
                }
                return results;
            }
        } catch (SQLException ex) {
            throw new UncheckedSQLException(
                String.format("could execute query!: %s", query),
                ex
            );
        }
    }
    
    /**
     * Saves the {@link List} of {@link Event}s into the connected PostgreSQL database.
     *
     * @param events to be saved into the database.
     */
    public void persist(List<Event> events) {
        for (Event event : events){
            handle(event);
        }
    }
    
    /**
     * Is called by the pipeline whenever a new Event is received.
     * This method persists the given {@link Event} value in the PostgreSQL database.
     *
     * @param event Event that is generated by the pipeline.
     */
    @Override
    public void handle(Event event) {
        // create statement with prepared fields to be filled
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT_QUERY)) {
            // fill the fields in the statement
            preparedStatement.setTimestamp(
                1,
                Timestamp.from(
                    Instant.ofEpochMilli(
                        event.getTimestamp()
                    )
                )
            );
            preparedStatement.setString(2, event.getEventName());
            preparedStatement.setString(3, event.getSource());
            String parameters = new String(parametersSerializer.serialize(new HashMap<>(event.getParameters())));
            preparedStatement.setObject(4,parameters);
            preparedStatement.setObject(5, new String(serializer.serialize(event)));
            
            // execute the insert
            int executeUpdate = preparedStatement.executeUpdate();
            
            // check if really saved
            if(executeUpdate != 1){
                throw new SQLException(String.format("Did not save the Event: %s", event));
            }
        } catch (SQLException ex) {
            throw new UncheckedSQLException("could not persist Events!", ex);
        }
    }
    
    /**
     * Closes the PostgreSQL connection.
     */
    @Override
    public void close(){
        try {
            connection.close();
        } catch (SQLException ex) {
            throw new UncheckedSQLException("could not close PostgreSQL connection!", ex);
        }
    }
}
