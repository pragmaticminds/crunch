package org.pragmaticminds.crunch.execution;

import java.io.Serializable;
import java.util.Collection;

/**
 * This interface describes the structure for a timestamp based sort function.
 * All incoming values are processed by the {@link #process(Serializable, Serializable, Serializable)} function and
 * stored into an internal queue. A Timer is than calling the {@link #onTimer(Serializable)} method to collect all
 * values that are over the given watermark timestamp.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.10.2018
 */
public interface SortFunction<I extends Serializable, T extends Serializable> extends Serializable {
    
    /**
     * Takes a value into the internal queue.
     *
     * @param timestamp relevant timestamp for sorting.
     * @param watermark current watermark timestamp as the top limiter.
     * @param value to be sorted by timestamp.
     */
    void process(I timestamp, I watermark, T value);
    
    /**
     * This method is called by a timer, that periodically collects results from the internal queue, that are over the
     * watermark timestamp and are ready to be processed.
     *
     * @param watermark timestamp, that indicates which queued values are ready to be processed in the next step.
     * @return a {@link Collection} of all values with a timestamp over the given watermark timestamp.
     */
    Collection<T> onTimer(I watermark);
}
