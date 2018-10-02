package org.pragmaticminds.crunch.events;

import java.io.Serializable;

/**
 * Basic interface for creation of Event objects.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 17.09.2018
 */
public interface EventBuilder<T extends Serializable> extends Serializable {
    
    /**
     * Builds an instance of the desired type
     *
     * @return a new instance of the desired type
     */
    T build();
}
