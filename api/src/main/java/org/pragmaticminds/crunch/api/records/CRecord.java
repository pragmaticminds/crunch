package org.pragmaticminds.crunch.api.records;

import java.io.Serializable;

/**
 * Generic Crunch Record (thus, CRecord).
 * It forms the base class of all possible Record Streams.
 *
 * Each Record has to have a timestamp.
 *
 * Usually subinterfaces of this interfaces should be used.
 *
 * @author julian
 * Created by julian on 14.08.18
 */
@SuppressWarnings("squid:S1609") // This is NO FunctionalInterface but rather a Marker Interface
public interface CRecord extends Serializable {

    /**
     * Getter
     *
     * @return Timestamp of record as ms since 01.01.1970
     */
    long getTimestamp();

}
