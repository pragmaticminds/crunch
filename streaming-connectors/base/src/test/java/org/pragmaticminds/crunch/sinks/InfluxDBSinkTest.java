package org.pragmaticminds.crunch.sinks;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.RecordHandler;
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
        RecordHandler sink = new InfluxDBSink(factory, "meas1");
        
        // Perform
        sink.init();
        sink.apply(createUntypedValues());
        sink.apply(createTypedValues());
        sink.close();
        
        // Verify that something has been done
        verify(mock, times(6)).write(any(Point.class));
    }
    
    private UntypedValues createUntypedValues() {
        HashMap<String, Object> values = new HashMap<>();
        values.put("value1", 1L);
        values.put("value2", "s");
        values.put("value3", 3.0);
        values.put("value4", true);
        values.put("value5", new Date(Instant.now().toEpochMilli()));
        return UntypedValues.builder()
            .source("source")
            .prefix("")
            .timestamp(0L)
            .values(values)
            .build();
    }
    
    private TypedValues createTypedValues() {
        return TypedValues.builder()
            .source("source")
            .timestamp(0L)
            .values(Collections.singletonMap("value", Value.of("String")))
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