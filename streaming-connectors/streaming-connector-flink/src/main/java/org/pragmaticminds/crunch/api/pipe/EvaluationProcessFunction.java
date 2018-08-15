package org.pragmaticminds.crunch.api.pipe;

import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

import java.util.List;

/**
 * Creates a {@link ProcessFunction} binded to a list of {@link EvaluationFunction}s from a SubStream.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class EvaluationProcessFunction extends ProcessFunction<MRecord, Event> {

    private List<EvaluationFunction> evaluationFunctions;
    
    /**
     * private constructor for the {@link Builder}
     * @param evaluationFunctions a list of {@link EvaluationFunction}s, that are to be integrated into
     *                            the processing of this class.
     */
    private EvaluationProcessFunction(List<EvaluationFunction> evaluationFunctions) {
        this.evaluationFunctions = evaluationFunctions;
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
    public void processElement(MRecord value, Context ctx, Collector<Event> out) throws Exception {
        // create evaluation context
        EvaluationContext evaluationContext = CollectorEvaluationContext.builder().withValue(value).withOut(out).build();
        
        // process the incoming values with the cloned evaluation functions
        for (EvaluationFunction evalFunction : this.evaluationFunctions) {
            evalFunction.eval(evaluationContext);
        }
        
        // all output events are already passed to the "out" Collector<Event>
    }
    
    /**
     * Creates instances of the {@link EvaluationProcessFunction} and checks ingoing parameters
     */
    public static final class Builder {
        private List<EvaluationFunction> evaluationFunctions;
    
        private Builder() {}
        
        public Builder withEvaluationFunctions(List<EvaluationFunction> evaluationFunctions) {
            this.evaluationFunctions = evaluationFunctions;
            return this;
        }
        
        public Builder but() { return builder().withEvaluationFunctions(evaluationFunctions); }
        
        public EvaluationProcessFunction build() {
            checkParameter(evaluationFunctions);
            return new EvaluationProcessFunction(evaluationFunctions);
        }
    
        private void checkParameter(List<EvaluationFunction> evaluationFunctions) {
            Preconditions.checkNotNull(evaluationFunctions);
            Preconditions.checkArgument(!evaluationFunctions.isEmpty());
        }
    }
}
