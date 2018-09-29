package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;

/**
 * Similar to an {@link EvaluationFunction} this class can be added to a {@link EvaluationPipeline}.
 * It can be implemented as a lambda, cause it has a {@link FunctionalInterface} annotation.
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 26.09.2018
 */
public interface RecordHandler extends Serializable {
    
    /**
     * Is called before start of evaluation.
     */
    void init();
    
    /**
     * evaluates the incoming {@link TypedValues} and stores it in the inner sink.
     * @param record contains incoming data
     */
    void apply(MRecord record);
    
    /**
     * Is called after last value is evaluated (if ever).
     */
    void close();
}
