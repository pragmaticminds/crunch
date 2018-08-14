package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a special {@link EvaluationContext} implementation to be used inside a {@link ChainedEvaluationFunction},
 * to separate it from the outer of {@link ChainedEvaluationFunction} context.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public abstract class StateEvaluationContext extends EvaluationContext {
    private final HashMap<String, Event>  events;
    private       TypedValues values;
    
    /**
     * private constructor on base of a {@link TypedValues} object
     * @param values
     */
    public StateEvaluationContext(TypedValues values){
        this.values = values;
        this.events = new HashMap<>();
    }
    
    // getter
    public Map<String, Event> getEvents() {
        return events;
    }
    
    /**
     * delivers the next {@link TypedValues} data to be processed
     *
     * @return the next record to be processed
     */
    @Override
    public TypedValues get() {
        return values;
    }
    
    /**
     * sets the current {@link TypedValues} data to be processed
     * @param values the record to be processed next
     */
    public void set(TypedValues values) {
        this.values = values;
    }
    
    /**
     * collects the resulting {@link Event} of processing with it's key
     * @param key should be unique, else it overrides the last value
     * @param event to be stored
     */
    public void collect(String key, Event event) {
        this.events.put(key, event);
    }
    
    /**
     * collects the resulting {@link Event} of processing
     * !! do not use the simple collect Method in the {@link ChainedEvaluationFunction} !!
     * @param event result of the processing of an {@link EvaluationFunction}
     * @deprecated this collect method is not to be used in the case of this class, instead collect(key, event) is to
     * be used.
     */
    @Override
    @Deprecated
    @SuppressWarnings("squid:S1133") // no reminder to remove the deprecated method needed
    public void collect(Event event) {
        this.events.put(Integer.toString(this.events.size()), event);
    }
}
