package org.pragmaticminds.crunch.api.windowed.extractor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.Aggregations;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.util.*;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.doubleChannel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class GroupByExtractorTest {
    
    private GroupByExtractor        extractor;
    private GroupByExtractor        extractor2;
    private ArrayList<MRecord>      records;
    private SimpleEvaluationContext context;
    private String                  myMin;
    
    @Before
    public void setUp() {
        // create extractors
        myMin = "my min";
        extractor = GroupByExtractor.builder()
            .aggregate(Aggregations.max(), doubleChannel("x"))
            .aggregate(Aggregations.max(), doubleChannel("x"))
            .aggregate(Aggregations.min(), doubleChannel("x"), myMin)
            .finalizer((aggregatedValues, context) ->
                    context.collect(
                        EventBuilder.anEvent()
                            .withEvent("test")
                            .withSource("test")
                            .withTimestamp(System.currentTimeMillis())
                            .withParameter("max.x", Value.of(aggregatedValues.get("max.x")))
                            .withParameter("max.x$0", Value.of(aggregatedValues.get("max.x$0")))
                            .withParameter("min.x", Value.of(aggregatedValues.get(myMin)))
                            .build()
                    )
            )
            .build();
    
        extractor2 = GroupByExtractor.builder()
            .aggregate(Aggregations.max(), doubleChannel("x"))
            .aggregate(Aggregations.min(), doubleChannel("x"), myMin)
            .build();
        
        // create records
        records = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        for (int i = 1; i < 11; i++) {
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put("x", (double)i);
            records.add(UntypedValues.builder()
                .prefix("")
                .source("test")
                .timestamp(timestamp)
                .values(valueMap)
                .build()
            );
        }
    }
    
    @Test
    public void extract() {
        context = new SimpleEvaluationContext(records.get(records.size()-1));
        
        records.forEach(record -> extractor.apply(record));
        extractor.finish(context);
        List<Event> results = context.getEvents();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Event event = results.get(0);
        Assert.assertNotNull(event);
        Assert.assertEquals(10D, event.getParameter("max.x").getAsDouble(), 0.0001);
        Assert.assertEquals(10D, event.getParameter("max.x$0").getAsDouble(), 0.0001);
        Assert.assertEquals(1D, event.getParameter("min.x").getAsDouble(), 0.0001);
    }
    
    @Test
    public void extractNoFinalizer() {
        context = new SimpleEvaluationContext(records.get(records.size()-1));
    
        records.forEach(record -> extractor2.apply(record));
        extractor2.finish(context);
        List<Event> results = context.getEvents();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Event event = results.get(0);
        Assert.assertNotNull(event);
        Assert.assertEquals(10D, event.getParameter("max.x").getAsDouble(), 0.0001);
        Assert.assertEquals(1D, event.getParameter(myMin).getAsDouble(), 0.0001);
    }
    
    @Test
    public void builder(){
        Assert.assertNotNull(extractor);
    }
}