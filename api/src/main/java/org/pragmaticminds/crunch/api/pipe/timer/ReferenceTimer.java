package org.pragmaticminds.crunch.api.pipe.timer;

/**
 * This implementation of the {@link Timer} is based on the reference time supplied by the data to be processed.
 * The tick method is called by a processing structure to update the current time, so that a timeout can be raised.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public abstract class ReferenceTimer implements Timer {
    
    private Long timeoutMs;
    private Long firstTimeStamp;
    
    /**
     * Starts a new timeout, that if raised should call the method onTimeout.
     * If a timeout is started before, that is canceled.
     *
     * @param timeoutMs the duration until timeout is raised.
     */
    @Override
    public void startTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
    
    /**
     * supplies the timer with a data based time value
     *
     * @param timestamp reference time to be checked if timeout is reached
     */
    @Override
    public void tick(long timestamp) {
        // ignore and return when timeout is not jet started
        if(timeoutMs == null){
            return;
        }
        // use the first timestamp to appear as reference for comparision
        if(firstTimeStamp == null){
            firstTimeStamp = timestamp;
            return;
        }
        // if timeout reached
        if(firstTimeStamp + timeoutMs <= timestamp){
            // deactivate the timer
            cancel();
            // raise timeout
            onTimeout();
        }
    }
    
    /**
     * cancels the current running timer.
     */
    @Override
    public void cancel() {
        firstTimeStamp = null;
        timeoutMs = null;
    }
}
