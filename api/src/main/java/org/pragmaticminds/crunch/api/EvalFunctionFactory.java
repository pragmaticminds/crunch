package org.pragmaticminds.crunch.api;

import java.io.Serializable;

/**
 * Factory Pattern to create EvalFunctions.
 *
 * @deprecated Part of the old API
 */
@Deprecated
@FunctionalInterface
public interface EvalFunctionFactory extends Serializable {

    EvalFunction create() throws IllegalAccessException;
}
