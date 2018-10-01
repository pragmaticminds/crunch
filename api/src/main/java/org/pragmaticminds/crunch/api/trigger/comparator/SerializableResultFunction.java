package org.pragmaticminds.crunch.api.trigger.comparator;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Same as {@link SerializableFunction} but without parameter only result.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
@FunctionalInterface
public interface SerializableResultFunction<R extends Serializable> extends Supplier<R>, Serializable {}
