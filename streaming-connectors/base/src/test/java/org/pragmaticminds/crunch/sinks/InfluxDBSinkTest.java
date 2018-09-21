package org.pragmaticminds.crunch.sinks;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class InfluxDBSinkTest {

    @Test
    public void callEval() throws InterruptedException {
        // Mock factory
        InfluxDB mock = Mockito.mock(InfluxDB.class);
        InfluxDBSink.InfluxFactory factory = () -> mock;

        // Create the EvalFunction
        EvaluationFunction sink = new InfluxDBSink(factory, "meas1");

        // Perform
        sink.init();
        sink.eval(createContext(createUntypedValues1()));
        sink.eval(createContext(createUntypedValues2()));
        sink.eval(createContext(createUntypedValues3()));
        sink.close();

        // Verify that something has been done
        verify(mock, times(4+7+3)).write(any(Point.class));
    }

    private UntypedValues createUntypedValues1() {
        HashMap<String, Object> values = new HashMap<>();
        values.put("M1", 34);
        values.put("M1_0", false);
        values.put("M1_2", false);
        values.put("M1_3", false);
        return UntypedValues.builder()
                .source("hemaScraper")
                .prefix("")
                .timestamp(1537279723248L)
                .values(values)
                .build();
    }

        private UntypedValues createUntypedValues2() {
            HashMap<String, Object> values = new HashMap<>();
                values.put("M1", -103);
                values.put("M1_0", true);
            values.put("M1_1", false);
            values.put("M1_2", false);
                values.put("M1_3", true);
                values.put("M1_4", true);
                values.put("M1_5", false);
                values.put("M1_7", true);


                return UntypedValues.builder()
                        .source("hemaScraper")
                        .prefix("")
                        .timestamp(1537279723525L)
                        .values(values)
                        .build();
            }

    private UntypedValues createUntypedValues3() {
        HashMap<String, Object> values = new HashMap<>();

        values.put("M1", -112);
        values.put("M1_0", false);
        values.put("M1_1", false);
        values.put("M1_2", false);
        values.put("M1_3", false);
        values.put("M1_4", true);
        values.put("M1_5", false);
        values.put("M1_7", true);

        return UntypedValues.builder()
                .source("hemaScraper")
                .prefix("")
                .timestamp(1537279723547L)
                .values(values)
                .build();
    }



    private EvaluationContext createContext(MRecord record) {
        return new EvaluationContext() {
            @Override
            public MRecord get() {
                return record;
            }

            @Override
            public void collect(Event event) {

            }
        };
    }
}
