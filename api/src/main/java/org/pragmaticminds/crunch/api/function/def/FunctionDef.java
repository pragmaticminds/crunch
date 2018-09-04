package org.pragmaticminds.crunch.api.function.def;

import org.pragmaticminds.crunch.api.records.DataType;

import java.io.Serializable;

/**
 * This represents the definition of an Evaluation Function.
 * It consists of a name of the function and a signature and a return type.
 * <p>
 * Created by julian on 14.02.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class FunctionDef implements Serializable {

    private DataType outputDataType;

    private String className;

    private Signature signature;

    private FunctionResults functionResults;

    public FunctionDef() {
        super();
    }

    public FunctionDef(Signature signature, Class evaluationFunctionClass, FunctionResults functionResults) {
        this.signature = signature;
        this.className = evaluationFunctionClass.getName();
        this.functionResults = functionResults;
        outputDataType = null;
    }

    public FunctionDef(Signature signature, DataType outputDataType,
                       Class evaluationFunctionClass, FunctionResults functionResults) {
        this.signature = signature;
        this.functionResults = functionResults;
        this.outputDataType = outputDataType;
        this.className = evaluationFunctionClass.getName();
    }

    public void setFunctionResults(FunctionResults functionResults) {
        this.functionResults = functionResults;
    }

    public String getClassName() {
        return this.className;
    }

    public Signature getSignature() {
        return signature;
    }

    public DataType getOutputDataType() {
        return outputDataType;
    }

    public void setOutputDataType(DataType outputDataType) {
        this.outputDataType = outputDataType;
    }

    public Class getEvaluationFunctionClass() throws ClassNotFoundException {
        return Class.forName(className);
    }

    @Override
    public String toString() {
        return "FunctionDef{" +
                "signature=" + signature +
                ", functionResults=" + functionResults +
                ", outputDataType=" + outputDataType +
                ", className='" + className + '\'' +
                '}';
    }
}
