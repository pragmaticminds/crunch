package org.pragmaticminds.crunch.runtime.sort;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Flink ProcessFunction that Sorts the incoming events.
 * The idea is taken from https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/CarEventSort.java.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class SortFunction extends ProcessFunction<MRecord, MRecord> {

    private static final Logger logger = LoggerFactory.getLogger(SortFunction.class);

    private int capacity = 100;
    private transient ValueState<PriorityQueue<MRecord>> queueState = null;

    public SortFunction() {
        /*
        for sonar
         */
    }

    public SortFunction(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Init a ValueStateDescriptor for the Flink Keyed State.
     *
     * @param config
     */
    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<PriorityQueue<MRecord>> descriptor = new ValueStateDescriptor<>(
                // state name
                "sorted-events",
                // type information of state
                TypeInformation.of(new TypeHint<PriorityQueue<MRecord>>() {
                }));
        queueState = getRuntimeContext().getState(descriptor);
    }

    /**
     * The Element is added to a {@link PriorityQueue} and a timer is set to check on time.
     * If the element is "too old", this means below the current watermark, it is discarded and a warning is logged.
     *
     * @param event
     * @param context
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(MRecord event, Context context, Collector<MRecord> out) throws Exception {
        TimerService timerService = context.timerService();

        if (context.timestamp() == null || context.timestamp() > timerService.currentWatermark()) {
            PriorityQueue<MRecord> queue = queueState.value();
            if (queue == null) {
                queue = new PriorityQueue<>(capacity, new ValueEventComparator());
            }
            queue.add(event);
            queueState.update(queue);
            timerService.registerEventTimeTimer(event.getTimestamp());
        } else {
            logger.warn("Event with old timestamp is discarded {}", event);
        }
    }

    /**
     * Checks if there are events that are "old enough" to be emitted (below the current watermark) and emits them, if so.
     *
     * @param timestamp
     * @param context
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<MRecord> out) throws Exception {
        PriorityQueue<MRecord> queue = queueState.value();
        Long watermark = context.timerService().currentWatermark();
        MRecord head = queue.peek();
        while (head != null && head.getTimestamp() <= watermark) {
            out.collect(head);
            queue.remove(head);
            head = queue.peek();
        }
    }

    /**
     * Comparator that compares the Long values of the two Timestamps.
     */
    public static class ValueEventComparator implements Comparator<MRecord> {

        /**
         * Use Long comapre on the timestamps of both events.
         *
         * @param o1
         * @param o2
         * @return
         */
        @Override
        public int compare(MRecord o1, MRecord o2) {
            return Long.compare(o1.getTimestamp(), o2.getTimestamp());
        }
    }
}

