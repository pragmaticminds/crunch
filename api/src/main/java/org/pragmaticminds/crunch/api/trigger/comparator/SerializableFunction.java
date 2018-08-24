package org.pragmaticminds.crunch.api.trigger.comparator;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Wrapper for the {@link Function} interface to make it {@link Serializable}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
@FunctionalInterface
public interface SerializableFunction<T extends Serializable, R extends Serializable> extends Function<T, R>, Serializable {}


