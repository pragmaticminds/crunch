package org.pragmaticminds.crunch.api;

import java.io.Serializable;

/**
 * A special way to define {@link EvalFunction}s. Definition is made by annotation.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.10.2017
 */
public interface AnnotatedEvalFunction<T> extends Serializable {
    /**
     * is called before the processing of records begins, to initialize the inner structure of the {@link EvalFunction}
     */
    void setup();

    /**
     * processes a single record and returns an output value of the set type
     *
     * @return output value of set type
     */
    T eval();

    /**
     * is called after processing of the records is finished
     */
    void finish();

    /**
     * This default function wrapps an {@link AnnotatedEvalFunction} as an {@link EvalFunction}, so that it can be
     * treated as one.
     *
     * @return this wrapped {@link AnnotatedEvalFunction} as an {@link EvalFunction}
     * @throws IllegalAccessException should not happen, when happens, than some classes are not visible from the
     *                                current context
     */
    default EvalFunction<T> asEvalFunction() throws IllegalAccessException {
        return new AnnotatedEvalFunctionWrapper(this.getClass());
    }
}
