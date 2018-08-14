package org.pragmaticminds.crunch.api.pipe;

import com.google.common.base.Preconditions;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.exceptions.TimerFactoryNotSetException;
import org.pragmaticminds.crunch.api.pipe.timer.Timer;
import org.pragmaticminds.crunch.api.pipe.timer.TimerFactory;
import org.pragmaticminds.crunch.api.values.TypedValues;
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
    private final TypedValues value;
    private final transient Collector<Event> out;
    private final TimerFactory timerFactory;
    
    /**
     * private constructor for the inner {@link Builder} class.
     * @param value the current one to be processed
     * @param out the outgoing result {@link Collector} of Flink
     * @param timerFactory creates a timer in a {@link Timer} fashion
     */
    private CollectorEvaluationContext(
        TypedValues value, Collector<Event> out, TimerFactory timerFactory
    ) {
        this.value = value;
        this.out = out;
        this.timerFactory = timerFactory;
    }
    
    /**
     * delivers the next {@link TypedValues} data to be processed
     *
     * @return the next record to be processed
     */
    @Override
    public TypedValues get() {
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
     * Creates a Timer that is boxed in {@link Timer} class object
     *
     * @param evaluationFunction
     * @return a Timer boxed in {@link Timer} class object
     * @throws TimerFactoryNotSetException is thrown, when it is not set in this instance of the class
     */
    @Override
    public Timer createNewTimer(EvaluationFunction evaluationFunction) {
        if(timerFactory != null){
            return timerFactory.create(evaluationFunction);
        }
        throw new TimerFactoryNotSetException();
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
        private TypedValues      value;
        private Collector<Event> out;
        private TimerFactory timerFactory;
        
        private Builder() {}
        
        public Builder withValue(TypedValues value) {
            this.value = value;
            return this;
        }
        
        public Builder withOut(Collector<Event> out) {
            this.out = out;
            return this;
        }
        
        public Builder withTimerFactory(TimerFactory timerFactory){
            this.timerFactory = timerFactory;
            return this;
        }
        
        public Builder but() { return builder().withValue(value).withOut(out); }
        
        public CollectorEvaluationContext build() {
            checkParameters();
            return new CollectorEvaluationContext(value, out, timerFactory);
        }
    
        private void checkParameters() {
            Preconditions.checkNotNull(value);
            Preconditions.checkNotNull(out);
        }
    }
}
