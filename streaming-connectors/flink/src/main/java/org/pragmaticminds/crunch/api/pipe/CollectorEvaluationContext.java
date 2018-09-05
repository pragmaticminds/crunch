package org.pragmaticminds.crunch.api.pipe;

import com.google.common.base.Preconditions;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

/**
 * This implementation of the EvaluationContext binds to a Flink {@link Collector}, so all collected {@link Event}s are
 * automatically in the output stream of Flink.
 * This class is created before a processing of {@link EvaluationFunction}s can be started.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class CollectorEvaluationContext extends EvaluationContext {
    private final MRecord value;
    private final transient Collector<Event> out;

    /**
     * private constructor for the inner {@link Builder} class.
     * @param value the current one to be processed
     * @param out the outgoing result {@link Collector} of Flink
     */
    private CollectorEvaluationContext(
            MRecord value, Collector<Event> out) {
        this.value = value;
        this.out = out;
    }
    
    /**
     * delivers the next {@link MRecord} data to be processed
     *
     * @return the next record to be processed
     */
    @Override
    public MRecord get() {
        return this.value;
    }
    
    /**
     * collects the resulting {@link Event}s of processing
     *
     * @param event result of the processing of an {@link EvaluationFunction}
     */
    @Override
    public void collect(Event event) {
        out.collect(event);
    }
    
    /**
     * Creates a builder for this class {@link CollectorEvaluationContext}
     * @return a builder for this class {@link CollectorEvaluationContext}
     */
    public static Builder builder() { return new Builder(); }
    
    /**
     * Creates instances of the type {@link CollectorEvaluationContext}
     */
    public static final class Builder {
        private MRecord value;
        private Collector<Event> out;

        private Builder() {}
        
        public Builder withValue(MRecord value) {
            this.value = value;
            return this;
        }
        
        public Builder withOut(Collector<Event> out) {
            this.out = out;
            return this;
        }
        
        public Builder but() { return builder().withValue(value).withOut(out); }
        
        public CollectorEvaluationContext build() {
            checkParameters();
            return new CollectorEvaluationContext(value, out);
        }
    
        private void checkParameters() {
            Preconditions.checkNotNull(value);
            Preconditions.checkNotNull(out);
        }
    }
}
