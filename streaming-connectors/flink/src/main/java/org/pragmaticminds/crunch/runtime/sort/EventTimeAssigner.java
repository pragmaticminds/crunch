package org.pragmaticminds.crunch.runtime.sort;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.pragmaticminds.crunch.events.GenericEvent;

/**
 * An assigner for Watermarks that allows a fixed time of "out of sync" time.
 * The time is given in ms in the constructur.
 * <p>
 * This means that an event which happend before another event has to arrive inside the given timewindow to be recognized.
 * <p>
 * It internally tracks the "maximum" time obsevet yet and emits a watermark that is behint this time with the given delay.
 * <p>
 * The description for this procedure is found in:
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/event_timestamps_watermarks.html#with-periodic-watermarks
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class EventTimeAssigner implements AssignerWithPunctuatedWatermarks<GenericEvent> {

    // Delay in ms
    private final long delayMs;

    // Maximum time observed yet
    private long currentMaxTimestamp;

    /**
     * Uses the given Delay.
     *
     * @param delayMs Delay for the out of sync messages in ms
     */
    public EventTimeAssigner(long delayMs) {
        this.delayMs = delayMs;
        this.currentMaxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public long extractTimestamp(GenericEvent event, long previousElementTimestamp) {
        long timestamp = event.getTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    /**
     * The next watermark is the current extracted timestamp minus the delay.
     *
     * @param event
     * @param extractedTimestamp
     * @return
     */
    @Override
    public Watermark checkAndGetNextWatermark(GenericEvent event, long extractedTimestamp) {
        // simply emit a watermark with every event
        return new Watermark(currentMaxTimestamp - delayMs);
    }
}