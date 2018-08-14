package org.pragmaticminds.crunch.api.pipe.timer;

import java.util.TimerTask;

/**
 * This is a simple implementation of a {@link Timer}.
 * It uses the {@link java.util.Timer} as the innerTimer.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 10.08.2018
 */
public abstract class SystemTimer implements Timer {
    private transient java.util.Timer innerTimer;
    
    /**
     * Starts a new timer, that if raised calls the method onTimeout.
     * If a timeout is started before, that is canceled.
     *
     * @param timeoutMs the duration until timeout is raised.
     */
    @Override
    public void startTimeout(long timeoutMs) {
        cancel();
        innerTimer = new java.util.Timer();
        innerTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                onTimeout();
            }
        }, timeoutMs);
    }
    
    /**
     * cancels the current running timer.
     */
    @Override
    public void cancel() {
        if(innerTimer == null){
            return;
        }
        innerTimer.cancel();
        innerTimer = null;
    }
}
