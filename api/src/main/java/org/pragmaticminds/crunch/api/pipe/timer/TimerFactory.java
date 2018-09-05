package org.pragmaticminds.crunch.api.pipe.timer;

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;

/**
 * This factory creates boxed timers in a {@link Timer} fashion.
 * That way every platform can use it's on type of timer.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 10.08.2018
 * @deprecated this is too complex - remove this
 */
@Deprecated
@FunctionalInterface
public interface TimerFactory extends Serializable {
    
    /**
     * This creates a boxed timer in a {@link Timer} fashion.
     * @param evaluationFunction the target with a onTimeout method, which is to be called when timeout is raised
     * @return a boxed timer in a {@link Timer} fashion
     */
    Timer create(EvaluationFunction evaluationFunction);
}
