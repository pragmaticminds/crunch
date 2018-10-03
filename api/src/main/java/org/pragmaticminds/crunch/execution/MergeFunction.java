package org.pragmaticminds.crunch.execution;

import java.io.Serializable;

/**
 * Interface for implementation of merge functions for pipelines
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.10.2018
 */
@FunctionalInterface
public interface MergeFunction<T extends Serializable, R extends Serializable> extends Serializable{
    
    /**
     * Merges incomming values to a aggregated value containing all other sub values from before.
     *
     * @param value to be merged
     * @return merged value
     */
    R merge(T value);
}
