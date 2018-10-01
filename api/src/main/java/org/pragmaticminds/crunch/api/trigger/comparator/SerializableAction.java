package org.pragmaticminds.crunch.api.trigger.comparator;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Wrapper for the {@link Consumer} interface to make it {@link Serializable}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
@FunctionalInterface
public interface SerializableAction<T extends Serializable> extends Consumer<T>, Serializable {}
