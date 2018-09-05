package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.io.Serializable;

/**
 * The context for evaluation in a {@link EvaluationFunction} containing ways to process incoming and
 * outgoing data.
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
@SuppressWarnings("squid:S1610") // not converting into an interface
public abstract class EvaluationContext implements Serializable {
    /**
     * delivers the next {@link MRecord} data to be processed
     * @return the next record to be processed
     */
    public abstract MRecord get();

    /**
     * collects the resulting {@link Event}s of processing
     * @param event result of the processing of an {@link EvaluationFunction}
     */
    public abstract void collect(Event event);
    
    /**
     * Getter for an {@link EventBuilder}
     * @return the Builder for {@link Event}s
     */
    public EventBuilder getEventBuilder(){
        return EventBuilder.anEvent();
    }
}
