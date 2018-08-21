package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;

/**
 * The general interface of all EvaluationFunctions.
 * It can be implemented as a lambda, cause it has a {@link FunctionalInterface} annotation.
 * It has a eval function for processing an {@link EvaluationContext}, which has an incoming value and handles outgoing
 * results for the {@link EvaluationFunction}.
 *
 * {@link #init()} and {@link #close()} methods added, thus usage of default.
 *
 * @author julian
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
@FunctionalInterface
public interface EvaluationFunction extends Serializable {

    /**
     * Is called before start of evaluation.
     */
    default void init() { /* Does nothing by default. */ }

    /**
     * evaluates the incoming {@link TypedValues} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context
     * @param ctx contains incoming data and a collector for the outgoing data
     */
    void eval(EvaluationContext ctx);
    
    /**
     * Is called after last value is evaluated (if ever).
     */
    default void close() { /* Does nothing by default. */ }


}
