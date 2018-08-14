package org.pragmaticminds.crunch.api.holder;

import org.pragmaticminds.crunch.api.mql.DataType;

import java.io.Serializable;

/**
 * This class is the basis for all holders (for all primitives / {@link DataType}.
 * Holders "hold" their Value to pass it into the Evaluation Function ({@see Holder Pattern}.
 * <p>
 * This class is Serializable but does not serialize it's current Value!
 * This is not necessary as it always "resettet" during the Evaluation.
 *
 * @author julian
 * Created by julian on 17.02.17.
 */
public class Holder<T> implements Serializable {

    private transient T value;
    private Class type;

    public Holder(T value, Class type) {
        this.value = value;
        this.type = type;
    }

    public Holder(Class type) {
        this.value = null;
        this.type = type;
    }

    public void set(T value) {
        this.value = value;
    }

    public T get() {
        return this.value;
    }

    public Class getType() {
        return type;
    }

}
