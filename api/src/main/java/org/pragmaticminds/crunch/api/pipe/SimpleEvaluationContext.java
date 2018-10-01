package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the {@link EvaluationContext} in a simple manner
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
public class SimpleEvaluationContext extends EvaluationContext {
    
    private final MRecord          values;
    private       ArrayList<Event> events;
    
    /**
     * Simple constructor, getting the values to be processed by a {@link EvaluationFunction}
     * @param values to be processed
     */
    public SimpleEvaluationContext(MRecord values) {
        this.values = values;
        this.events = new ArrayList<>();
    }

    /** @inheritDoc */
    @Override
    public MRecord get() {
        return values;
    }
    
    public List<Event> getEvents(){
        return this.events;
    }

    /** @inheritDoc */
    @Override
    public void collect(Event event) {
        if(event != null){
            events.add(event);
        }
    }
}
