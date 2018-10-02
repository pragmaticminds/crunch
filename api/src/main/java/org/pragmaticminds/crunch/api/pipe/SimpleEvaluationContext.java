package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the {@link EvaluationContext} in a simple manner
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
public class SimpleEvaluationContext<T extends Serializable> extends EvaluationContext<T> {

    private final MRecord values;
    private ArrayList<T> events;

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

    public List<T> getEvents() {
        return this.events;
    }

    /** @inheritDoc */
    @Override
    public void collect(T event) {
        if(event != null){
            events.add(event);
        }
    }
}
