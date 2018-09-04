package org.pragmaticminds.crunch.api.function.def;

import org.pragmaticminds.crunch.api.records.DataType;

import java.io.Serializable;

/**
 * Represents an FunctionParameter of the Signature
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class FunctionParameter implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private FunctionParameterType parameterType;

    private DataType dataType;

    public FunctionParameter() {
        super();
    }

    public FunctionParameter(String name, FunctionParameterType parameterType, DataType dataType) {
        this.name = name;
        this.parameterType = parameterType;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public FunctionParameterType getParameterType() {
        return parameterType;
    }

    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "FunctionParameter{" +
                "name='" + name + '\'' +
                ", parameterType=" + parameterType +
                ", dataType=" + dataType +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FunctionParameter parameter = (FunctionParameter) o;

        return getParameterType() == parameter.getParameterType() && getDataType() == parameter.getDataType();
    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getParameterType() != null ? getParameterType().hashCode() : 0);
        result = 31 * result + (getDataType() != null ? getDataType().hashCode() : 0);
        return result;
    }
}
