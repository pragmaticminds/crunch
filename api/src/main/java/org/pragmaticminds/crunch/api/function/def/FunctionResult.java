package org.pragmaticminds.crunch.api.function.def;

import org.pragmaticminds.crunch.api.mql.DataType;

import java.io.Serializable;

/**
 * Contains the Information of the "result table" which the function will produce.
 * This means all names of columns and their datatypes.
 * <p>
 * Created by julian on 07.04.17.
 */
public class FunctionResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private DataType dataType;

    private String name;

    public FunctionResult() {
        super();
    }

    public FunctionResult(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return dataType;
    }
}
