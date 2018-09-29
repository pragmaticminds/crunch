package org.pragmaticminds.crunch.api.pipe;

/**
 * Implements the default methods {@link #init()} and {@link #close()}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public abstract class AbstractRecordHandler implements RecordHandler {
    /** Is called before start of evaluation. */
    @Override
    public void init() {
        /* default do nothing */
    }
    
    /** Is called after last value is evaluated (if ever). */
    @Override
    public void close() {
        /* default do nothing */
    }
}
