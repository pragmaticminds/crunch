package org.pragmaticminds.crunch.sinks;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
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
public class InfluxDBSink implements EvaluationFunction {

    private final InfluxFactory factory;
    private final String measurement;

    private transient InfluxDB influxDB = null;

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


    @Override
    public void eval(EvaluationContext ctx) {
        MRecord record = ctx.get();
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
            PointGeneratingVisitor visitor = new PointGeneratingVisitor(values.getTimestamp(), measurement, values.getSource(), entry.getKey());
            // Write (Batched mode is active)
            influxDB.write(entry.getValue().accept(visitor));
        }
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
            }
            else{
                influxDB = InfluxDBFactory.connect(url);
            }

            checkOrCreateDatabaseIfNotExists(influxDB, db);
            influxDB.setDatabase(db);
            influxDB.enableBatch(maxNumberOfBatchPoints, commitTimeMaxMs, TimeUnit.MILLISECONDS);
            influxDB.enableGzip();
            return influxDB;
        }
    }
}