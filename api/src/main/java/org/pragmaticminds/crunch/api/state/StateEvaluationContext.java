package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a special {@link EvaluationContext} implementation to be used inside a {@link MultiStepEvaluationFunction},
 * to separate it from the outer of {@link MultiStepEvaluationFunction} context.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class StateEvaluationContext extends EvaluationContext {
    private final HashMap<String, Event>  events;
    private MRecord values;
    private String alias;
    
    /**
     * private constructor on base of a {@link MRecord} object
     * @param values
     */
    public StateEvaluationContext(MRecord values, String alias){
        this.values = values;
        this.alias = alias;
        this.events = new HashMap<>();
    }
    
    // getter
    public Map<String, Event> getEvents() {
        return events;
    }
    
    /**
     * delivers the next {@link MRecord} data to be processed
     *
     * @return the next record to be processed
     */
    @Override
    public MRecord get() {
        return values;
    }
    
    // setter
    /**
     * sets the current {@link MRecord} data to be processed
     * @param values the record to be processed next
     */
    public void set(MRecord values) {
        this.values = values;
    }
    
    /**
     * sets the alias for naming resulting {@link Event}s.
     * @param alias name for resulting Events.
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    // methods
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
     * !! do not use the simple collect Method in the {@link MultiStepEvaluationFunction} !!
     * @param event result of the processing of an {@link EvaluationFunction}
     * @deprecated this collect method is not to be used in the case of this class, instead collect(key, event) is to
     * be used.
     */
    @Override
    @Deprecated
    @SuppressWarnings("squid:S1133") // no reminder to remove the deprecated method needed
    public void collect(Event event) {
        String key;
        if(this.events.size() == 0){
            key = alias;
        }else{
            key = String.format("%s%s", alias, Integer.toString(this.events.size()-1));
        }
        this.events.put(key, event);
    }
}
