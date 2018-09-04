package org.pragmaticminds.crunch.api.function.def;

import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.annotations.ResultType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entity to save Results fired from an {@link EvalFunction} in processing
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.05.2017.
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class FunctionResults implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<FunctionResult> results;

    public FunctionResults() {
        super();
    }

    public FunctionResults(ResultType[] resultTypes) {
        this.results = new ArrayList<>();
        Arrays.stream(resultTypes).forEach(rt -> results.add(new FunctionResult(rt.name(), rt.dataType())));
    }

    public FunctionResults(List<FunctionResult> results) {
        this.results = results;
    }

    public List<FunctionResult> getResults() {
        return results;
    }

    public void setResults(List<FunctionResult> results) {
        this.results = results;
    }
}
