package org.pragmaticminds.crunch.api;

import org.pragmaticminds.crunch.api.annotations.ChannelValue;
import org.pragmaticminds.crunch.api.function.def.FunctionParameter;
import org.pragmaticminds.crunch.api.function.def.Signature;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Call to an Evaluation function.
 * This call knows the context and has thus e.g. the literal parameters given in the call.
 * And it contains a mapping of the channel names, the mapping is organized the following way:
 * name of the channel (in the @{@link ChannelValue}-annotation
 * - name of the channel in "reality"
 * <p>
 * Furthermore it contains a direct reference to the evaluation function that has been called.
 *
 * @author julian
 * Created by julian on 05.11.17
 */
@SuppressWarnings("squid:S1319") // Use HashMap instead of map to be serializable
public class EvalFunctionCall implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(EvalFunctionCall.class);

    private EvalFunctionFactory factory;
    private EvalFunction evalFunction;
    private HashMap<String, Value> literals;
    private HashMap<String, String> channelNamesMapping;

    public EvalFunctionCall(EvalFunction evalFunction, Map<String, Value> literals, Map<String, String> channelNamesMapping) {
        this.evalFunction = evalFunction;
        setLiterals(literals);
        setChannelNamesMapping(channelNamesMapping);
    }

    public EvalFunctionCall(EvalFunctionFactory factory, Map<String, Value> literals, Map<String, String> channelNamesMapping) {
        this.factory = factory;
        setLiterals(literals);
        setChannelNamesMapping(channelNamesMapping);
    }

    public EvalFunction getEvalFunction() {
        if (this.factory != null) {
            try {
                return factory.create();
            } catch (IllegalAccessException e) {
                logger.error("Exception occured", e);
                return null;
            }
        }
        return evalFunction;
    }

    public void setEvalFunction(EvalFunction evalFunction) {
        this.evalFunction = evalFunction;
    }

    /*
    this method has to return HashMap instead of Map to be serializable
     */
    public HashMap<String, Value> getLiterals() {
        return literals;
    }

    public void setLiterals(Map<String, Value> literals) {
        this.literals = new HashMap<>(literals);
    }

    public Map<String, String> getChannelNamesMapping() {
        return channelNamesMapping;
    }

    public void setChannelNamesMapping(Map<String, String> channelNamesMapping) {
        this.channelNamesMapping = new HashMap<>(channelNamesMapping);
    }

    /**
     * Returns a Map with the Channel names (as in the real world measurement) and the {@link DataType} extracted from the {@link Signature}
     * of the Eval Function.
     *
     * @return Mapping of Channel Names to {@link DataType}
     */
    public Map<String, DataType> getChannelsAndTypes() {
        return channelNamesMapping.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, entry -> {
                    Optional<FunctionParameter> fp = Arrays.stream(getEvalFunction().getFunctionDef().getSignature().getParameters())
                            .filter(functionParameter -> entry.getKey().equals(functionParameter.getName()))
                            .findFirst();
                    if (!fp.isPresent()) {
                        throw new IllegalStateException("Asking for a channel \"" + entry.getKey() + "\" that is not present in the functions signature!");
                    }

                    return fp.get().getDataType();
                }));
    }

}
