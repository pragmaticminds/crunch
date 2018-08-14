package org.pragmaticminds.crunch.api.values;

import java.io.Serializable;

/**
 * Generic Interface for all Events that have a timestamp.
 * Some runtime functions (like sorting) are only dependent on the timestamp and can thus be implemented more generally.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public interface ValueEvent extends Serializable {

    long getTimestamp();

    String getSource();

}
