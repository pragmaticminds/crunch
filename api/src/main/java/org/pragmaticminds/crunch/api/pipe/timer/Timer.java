package org.pragmaticminds.crunch.api.pipe.timer;

import java.io.Serializable;

/**
 * A boxing construct to be able to use Timer functionality supplied by the framework in use as the Crunch pipeline
 * implementation. It can be implemented as a system time based Timer or one where the time value is supplied by the
 * data, that is processed.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 10.08.2018
 */
public interface Timer extends Serializable {
    
    /**
     * Starts a new timeout, that if raised should call the method onTimeout.
     * If a timeout is started before, that is canceled.
     * @param timeoutMs the duration until timeout is raised.
     */
    void startTimeout(long timeoutMs);
    
    /**
     * supplies the timer with a data based time value.
     * !!! Only needed to implemented if the time value comes from references and not from the system.
     *
     * @param timestamp reference time to be checked if timeout is reached
     */
    default void tick(long timestamp){}
    
    /**
     * cancels the current running timer.
     */
    void cancel();
    
    /**
     * This is called when the timeout is raised.
     */
    void onTimeout();
}
