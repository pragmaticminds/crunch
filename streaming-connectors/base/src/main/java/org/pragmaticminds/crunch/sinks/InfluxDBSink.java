/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.sinks;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.pragmaticminds.crunch.api.pipe.AbstractRecordHandler;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implemementation of an InfluxDBSink as {@link EvaluationFunction}.
 * This is not really the nicest way but makes it easy to use it with the CrunchPipeline.
 * <p>
 * Implementation is based on the InfluxDBSink from inacore-parten.
 * <p>
 * injects incoming data into InfluxDb
 * information see
 * <a href="https://github.com/dataArtisans/oscon">https://github.com/dataArtisans/oscon</a>
 * <a href="https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/utils/influxdb/InfluxDBSink.java">https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/utils/influxdb/InfluxDBSink.java</a>
 * <p>
 * code based on:
 * <a href="https://github.com/dataArtisans/oscon/blob/master/src/main/java/com/dataartisans/sinks/InfluxDBSink.java">https://github.com/dataArtisans/oscon/blob/master/src/main/java/com/dataartisans/sinks/InfluxDBSink.java</a>
 *
 * @author julian
 * Created on 15.08.18
 */
public class InfluxDBSink extends AbstractRecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBSink.class);

    private final InfluxFactory factory;
    private final String measurement;

    private transient InfluxDB influxDB = null;

    private HashMap<String, Value> lastValues = new HashMap<>();

    /**
     * Default constructor
     *
     * @param factory     Factory to create the InflxuDB Object
     * @param measurement the name of the measurement where incoming data shall be stored
     */
    public InfluxDBSink(InfluxFactory factory, String measurement) {
        this.factory = factory;
        this.measurement = measurement;
    }

    /**
     * intializes sink.
     */
    @Override
    public void init() {
        this.influxDB = factory.create();
    }

    /**
     * processes the incoming {@link TypedValues} and stores it in the inner sink.
     *
     * @param record contains incoming data
     */
    @Override
    public void apply(MRecord record) {
        TypedValues values;
        if (record.getClass().isAssignableFrom(UntypedValues.class)) {
            values = ((UntypedValues) record).toTypedValues();
        } else if (record.getClass().isAssignableFrom(TypedValues.class)) {
            values = (TypedValues) record;
        } else {
            throw new UnsupportedOperationException("Currently only UntypedValues and Typed values " +
                    "are supported in InfluxDBSink!");
        }

        for (Map.Entry<String, Value> entry : values.getValues().entrySet()) {
            //only write the new values or the values that changed
            if(!lastValues.containsKey(entry.getKey()) ||
                    (lastValues.containsKey(entry.getKey()) && !lastValues.get(entry.getKey()).equals(entry.getValue()))) {
                PointGeneratingVisitor visitor = new PointGeneratingVisitor(values.getTimestamp(), measurement, values.getSource(), entry.getKey());
                // Write (Batched mode is active)
                influxDB.write(entry.getValue().accept(visitor));
            }
            lastValues.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Collects all channel identifiers, that are used for the triggering condition.
     * In this case no identifiers can be returned.
     *
     * @return a {@link List} or {@link Collection} of all channel identifiers from triggering
     */
    @Override
    public Set<String> getChannelIdentifiers() {
        return Collections.emptySet();
    }

    @FunctionalInterface
    interface InfluxFactory extends Serializable {

        /**
         * Creates and returns an Instance of {@link InfluxDB}
         *
         * @return Valid InfluxDB Object.
         */
        InfluxDB create();

    }

    /**
     * implements the type conversion for storage into InfluxDb, based on VisitorPattern
     */
    private static class PointGeneratingVisitor implements ValueVisitor<Point> {

        private long timestamp;
        private String source;
        private String measurement;
        private String field;

        public PointGeneratingVisitor(long timestamp, String measurement, String source, String field) {
            this.timestamp = timestamp;
            this.source = source;
            this.measurement = measurement;
            this.field = field;
        }

        @Override
        public Point visit(BooleanValue value) {
            Point.Builder builder = createPointBuilder()
                    .addField(field, value.getAsBoolean());
            return builder.build();
        }

        @Override
        public Point visit(DateValue value) {
            Point.Builder builder = createPointBuilder()
                    .addField(field, value.getAsDate().toString());
            return builder.build();
        }


        @Override
        public Point visit(DoubleValue value) {
            Point.Builder builder = createPointBuilder()
                    .addField(field, value.getAsDouble());
            return builder.build();
        }

        @Override
        public Point visit(LongValue value) {
            Point.Builder builder = createPointBuilder()
                    .addField(field, value.getAsLong());
            return builder.build();
        }

        @Override
        public Point visit(StringValue value) {
            Point.Builder builder = createPointBuilder()
                    .addField(field, value.getAsString());
            return builder.build();
        }

        private Point.Builder createPointBuilder() {
            return Point
                    .measurement(measurement)
                    .tag("source", source)
                    .time(timestamp, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Default Factory.
     */
    public static class DefaultInfluxFactory implements InfluxFactory {

        private final String url;
        private final String db;
        private final String influxUser;
        private final String influxPass;
        private int maxNumberOfBatchPoints;
        private int commitTimeMaxMs;

        /**
         * Constructor without use of user/password
         * user/password where set to empty string
         * @param url                    influx urls incl. port
         * @param db                     influx db where data shall be stored
         * @param maxNumberOfBatchPoints Number of Points per Batch
         * @param commitTimeMaxMs        Timeout between forcing batch commits
         */
        public DefaultInfluxFactory(String url, String db, int maxNumberOfBatchPoints, int commitTimeMaxMs) {
            this(url,db,"","",maxNumberOfBatchPoints,commitTimeMaxMs);
        }

        /**
         *
         * @param url                       influx urls incl. port
         * @param db                        influx db where data shall be stored
         * @param influxUser                username to connected to influx server
         * @param influxPass                password to connect to influx server
         * @param maxNumberOfBatchPoints    Number of Points per Batch
         * @param commitTimeMaxMs           Timeout between forcing batch commits
         */
        public DefaultInfluxFactory(String url, String db, String influxUser, String influxPass, int maxNumberOfBatchPoints, int commitTimeMaxMs) {
            this.url = url;
            this.db = db;
            this.maxNumberOfBatchPoints = maxNumberOfBatchPoints;
            this.commitTimeMaxMs = commitTimeMaxMs;
            this.influxUser = influxUser;
            this.influxPass = influxPass;
        }

        /**
         * checks if database is existing and creates it if not
         *
         * @param influxDB     influx object holding connection
         * @param databaseName Name of the DB to check
         */
        private static void checkOrCreateDatabaseIfNotExists(InfluxDB influxDB, String databaseName) {
            List<String> dbNames = influxDB.describeDatabases();

            if (!dbNames.contains(databaseName)) {
                influxDB.createDatabase(databaseName);
            }
        }

        @Override
        public InfluxDB create() {
            InfluxDB influxDB;
            if(!influxUser.isEmpty()){
                influxDB = InfluxDBFactory.connect(url,influxUser,influxPass);
            } else{
                influxDB = InfluxDBFactory.connect(url);
            }

            logger.trace("Influx created: {} {} {} {}", url, db, maxNumberOfBatchPoints, commitTimeMaxMs);
            checkOrCreateDatabaseIfNotExists(influxDB, db);
            influxDB.setDatabase(db);
            influxDB.enableBatch(maxNumberOfBatchPoints, commitTimeMaxMs, TimeUnit.MILLISECONDS);
            influxDB.enableGzip();
            return influxDB;
        }
    }
}