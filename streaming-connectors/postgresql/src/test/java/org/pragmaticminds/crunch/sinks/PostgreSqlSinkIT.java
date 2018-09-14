package org.pragmaticminds.crunch.sinks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Connection data for Discard
 *   postgres.url=jdbc:postgresql://192.168.169.13:5439/testDb
 *   postgres.user=pm
 *   postgres.password=Minds2017
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 11.09.2018
 */
public class PostgreSqlSinkIT {
    private static final Logger logger = LoggerFactory.getLogger(PostgreSqlSinkIT.class);
    
    private static String url  = "jdbc:postgresql://192.168.169.13:5439/testDb";
    private static String user = "pm";
    private static String pass = "Minds2017";
    
    private static String sqlDropEventsTable = "DROP TABLE IF EXISTS events";
    
    @Before
    public void setUp() {
        // drop events table before start test
        try (PostgreSqlSink postgreSqlSink = new PostgreSqlSink(url, user, pass)) {
            postgreSqlSink.executeCommand(sqlDropEventsTable);
        }
    }
    
    @After
    public void tearDown() {
        // drop events table after test
        try (PostgreSqlSink postgreSqlSink = new PostgreSqlSink(url, user, pass)) {
            postgreSqlSink.executeCommand(sqlDropEventsTable);
        }
    }
    
    @Test
    public void run() {
        connectAndCreateEvents();
        connectAndQueryAllEvents();
    }
    
    public void connectAndQueryAllEvents() {
        try (PostgreSqlSink sink = new PostgreSqlSink(url, user, pass)) {
            List<Event> results = sink.executeQuery("SELECT * FROM events");
            for (Event event : results){
                logger.debug("event: {}", event);
            }
        }
    }
    
    public void connectAndCreateEvents() {
        try (PostgreSqlSink sink = new PostgreSqlSink(url, user, pass)) {
            List<Event> events = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                events.add(
                    EventBuilder.anEvent()
                        .withTimestamp(System.currentTimeMillis() + i)
                        .withEvent("TestEvent")
                        .withSource("TestSource")
                        .withParameter("string", "string")
                        .withParameter("long", 123L)
                        .withParameter("double", 0.123D)
                        .withParameter("i", (long)i)
                        .build()
                );
            }
            sink.persist(events);
        }
    }
}