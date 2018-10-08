package org.pragmaticminds.crunch.execution;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.TimerGraphStageLogic;
import org.pragmaticminds.crunch.api.pipe.SubStream;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Realizes the sorting of incoming records by timestamps in a defined sort window.
 * Records outside the defined window a discarded.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 04.10.2018
 */
public class SortGraphFlow<T extends MRecord> extends GraphStage<FlowShape<T, T>> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SortGraphFlow.class);
    
    // constructor parameters
    private final Long watermarkOffsetMs;
    
    private final transient Inlet<T> in = Inlet.<T>create("SortGraphFlow.in");
    private final transient Outlet<T> out = Outlet.<T>create("SortGraphFlow.out");
    private final FlowShape<T, T> shape = FlowShape.of(in, out);
    
    private TimestampSortFunction<T> sortFunction;
    private final Serializable bufferMutex = new Serializable() {};
    private final ArrayDeque<T> buffer = new ArrayDeque<>();
    
    
    /**
     * Main constructor taking the {@link SubStream} structure and the watermark offset in milly seconds.
     *
     * @param watermarkOffsetMs defines the sort window
     */
    public SortGraphFlow(Long watermarkOffsetMs) {
        this.watermarkOffsetMs = watermarkOffsetMs;
        sortFunction = new TimestampSortFunction<>();
    }
    
    /** {@inheritDoc} */
    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }
    
    /**
     * Creates a Graph with an inner Sink and Source.
     * The {@link TimestampSortFunction} is also integrated in the processing.
     *
     * @param inheritedAttributes are ignored
     * @return the {@link GraphStageLogic}
     * @throws Exception if something went wrong
     */
    @Override
    @SuppressWarnings({
        "squid:S3776", // do not refactor to reduce cognitive complexity
        "squid:S1171" // have to use field initializers in here
    })
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        // create a GraphStageLogic with timer functionality
        return new TimerGraphStageLogic(shape()) {
            // All state MUST be inside the GraphStageLogic,
            // never inside the enclosing GraphStage.
            // This state is safe to access and modify from all the
            // callbacks that are provided by GraphStageLogic and the
            // registered handlers.
            
            private AtomicReference<Long> systemTimeToRecordTimeDifference = new AtomicReference<>();
    
            // Initialization in Akka Java is usually done in static init blocks
            {
                // set the handlers for onPush and onPull
                setHandlers(in, out, new AbstractInOutHandler() {
                    /**
                     * Is called by the in when a record is available.
                     *
                     * @throws Exception if something went wrong.
                     */
                    @Override
                    public void onPush() throws Exception {
                        T record = grab(in);
                        // for calculating the relative time difference
                        systemTimeToRecordTimeDifference.set(System.currentTimeMillis() - record.getTimestamp());
                        sortFunction.process(record.getTimestamp(), calculateWatermark(record.getTimestamp()), record);
                        scheduleOnce("key", Duration.of(watermarkOffsetMs, ChronoUnit.MILLIS));
                    }
    
                    /**
                     * Is called by the out back pressure.
                     *
                     * @throws Exception if something went wrong
                     */
                    @Override
                    public void onPull() throws Exception {
                        T record;
                        if((record = bufferPop()) == null){
                            if(!isClosed(in) && !hasBeenPulled(in)){
                                pull(in);
                            }
                        }else{
                            push(out, record);
                        }
                    }
                });
            }
    
            /**
             * Tries to get a record from the #buffer synchronized.
             *
             * @return a record if present otherwise null.
             */
            private T bufferPop(){
                synchronized (bufferMutex) {
                    if(buffer.isEmpty()){
                        return null;
                    }
                    return buffer.pop();
                }
            }
    
            /**
             * Is always called when a Timer created by scheduleOnce is reached.
             *
             * @param key is ignored
             */
            @Override
            public void onTimer(Object key){
                // calculate watermark basing on the last difference of system time and record time
                Long actualTimestamp = System.currentTimeMillis();
                if(systemTimeToRecordTimeDifference.get() != null){
                    actualTimestamp -= systemTimeToRecordTimeDifference.get();
                }
                
                // call onTimer to get all messages over the watermark
                Collection<T> results = sortFunction.onTimer(calculateWatermark(actualTimestamp));
                if(!results.isEmpty()){
                    synchronized (bufferMutex) {
                        results.forEach(buffer::push);
                    }
                }
                T record1 = bufferPop();
                if(record1 == null && !isClosed(in) && !hasBeenPulled(in)){
                    pull(in);
                }
                while (record1 != null){
                    push(out, record1);
                    record1 = bufferPop();
                }
            }
    
            /**
             * Calculates the current watermark by record.getTimestamp() - watermarkOffsetMs.
             *
             * @return current watermark
             * @param timestamp relative time to records timestamps
             */
            private long calculateWatermark(long timestamp) {
                return timestamp - watermarkOffsetMs;
            }
    
            /** {@inheritDoc} */
            @Override
            public void preStart() throws Exception {
                super.preStart();
                pull(in);
                logger.debug("Initializing SortGraphFlow");
            }
    
            /** {@inheritDoc} */
            @Override
            public void postStop() throws Exception {
                super.postStop();
                logger.debug("Closing SortGraphFlow");
            }
        };
    }
}
