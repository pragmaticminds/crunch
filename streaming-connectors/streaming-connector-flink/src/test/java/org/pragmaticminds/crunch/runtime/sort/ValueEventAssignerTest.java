package org.pragmaticminds.crunch.runtime.sort;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the value assigner.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValueEventAssignerTest {

    private ValueEventAssigner assigner;

    @Before
    public void setUp() {
        assigner = new ValueEventAssigner(100);
    }

    @Test
    public void extractTimestamp_works() {
        long extractTimestamp = assigner.extractTimestamp(UntypedValues.builder().timestamp(10L).build(), -1);

        assertEquals(10L, extractTimestamp);
    }

    @Test
    public void checkAndGetNextWatermark_works() {
        UntypedValues event = UntypedValues.builder().timestamp(10L).build();
        assigner.extractTimestamp(event, -1);
        Watermark watermark = assigner.checkAndGetNextWatermark(event, event.getTimestamp());

        assertEquals(-90, watermark.getTimestamp());
    }
}