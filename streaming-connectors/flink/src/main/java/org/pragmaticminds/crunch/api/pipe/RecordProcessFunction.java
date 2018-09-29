package org.pragmaticminds.crunch.api.pipe;

import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;

import java.util.List;

/**
 * Creates a {@link ProcessFunction} binded to a list of {@link RecordHandler}s from a SubStream.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class RecordProcessFunction extends ProcessFunction<MRecord, Void> {
    
    private List<RecordHandler> recordHandlers;
    
    /**
     * private constructor for the {@link Builder}
     * @param recordHandlers a list of {@link RecordHandler}s, that are to be integrated into
     *                       the processing of this class.
     */
    private RecordProcessFunction(List<RecordHandler> recordHandlers) {
        this.recordHandlers = recordHandlers;
    }
    
    /**
     * Creates a new instance of the {@link Builder}
     * @return a new instance of the {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter
     * and also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting
     *              a {@link TimerService} for registering timers and querying the time. The
     *              context is only valid during the invocation of this method, do not store it.
     * @param out   The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    @Override
    public void processElement(MRecord value, Context ctx, Collector<Void> out) throws Exception {
        // process the incoming values with the cloned evaluation functions
        for (RecordHandler recordHandler : this.recordHandlers) {
            recordHandler.apply(value);
        }
    }
    
    /**
     * Creates instances of the {@link RecordProcessFunction} and checks ingoing parameters
     */
    public static final class Builder {
        private List<RecordHandler> recordHandlers;
        
        private Builder() {}
        
        public Builder withRecordHandlers(List<RecordHandler> recordHandlers) {
            this.recordHandlers = recordHandlers;
            return this;
        }
        
        public Builder but() { return builder().withRecordHandlers(recordHandlers); }
        
        public RecordProcessFunction build() {
            checkParameter(recordHandlers);
            return new RecordProcessFunction(recordHandlers);
        }
        
        private void checkParameter(List<RecordHandler> recordHandlers) {
            Preconditions.checkNotNull(recordHandlers);
            Preconditions.checkArgument(!recordHandlers.isEmpty());
        }
    }
}
