package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;

/**
 * The context for evaluation in a {@link EvaluationFunction} containing ways to process incoming and
 * outgoing data.
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
@SuppressWarnings("squid:S1610") // not converting into an interface
public abstract class EvaluationContext<T extends Serializable> implements Serializable {
    /**
     * delivers the next {@link MRecord} data to be processed
     * @return the next record to be processed
     */
    public abstract MRecord get();

    /**
     * collects the resulting Event object of processing
     * @param event result of the processing of an {@link EvaluationFunction}
     */
    public abstract void collect(T event);
}
