package org.pragmaticminds.crunch.api;

import org.pragmaticminds.crunch.api.annotations.*;
import org.pragmaticminds.crunch.api.events.EventHandler;
import org.pragmaticminds.crunch.api.function.def.*;
import org.pragmaticminds.crunch.api.holder.Holder;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Wrapps an {@link AnnotatedEvalFunction} and lets it look like an {@link EvalFunction}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.10.2017
 *
 * @deprecated Part of the old API
 */
@Deprecated
public class AnnotatedEvalFunctionWrapper<T> extends EvalFunction<T> {
    private static final transient Logger logger = LoggerFactory.getLogger(AnnotatedEvalFunctionWrapper.class);
    private final String name;
    private final DataType outputDataType;
    private final List<FunctionParameter> parameters = new ArrayList<>();
    private FunctionResults functionResults = null;
    private Class<? extends AnnotatedEvalFunction<T>> annotatedEvalFunctionClass;
    private AnnotatedEvalFunction<T> annotatedEvalFunctionInstance;
    private HashMap<String, Holder> literalValueHolder = new HashMap<>();
    private HashMap<String, DataType> literalTypeHolder = new HashMap<>();
    private HashMap<String, Holder> channelValueHolder = new HashMap<>();
    private Holder timeValueHolder = null;
    private HashMap<String, DataType> channelTypeHolder = new HashMap<>();

    public AnnotatedEvalFunctionWrapper(Class<? extends AnnotatedEvalFunction<T>> annotatedEvalFunctionClass) throws IllegalAccessException {
        this.annotatedEvalFunctionClass = annotatedEvalFunctionClass;

        // create instance of the class
        try {
            this.annotatedEvalFunctionInstance = annotatedEvalFunctionClass.newInstance();
        } catch (InstantiationException e) {
            logger.error("Could not create new instance of AnnotatedEvalFunction class.", e);
        }


        // getValue queryable name of the function class
        EvaluationFunction evalAnnotation = annotatedEvalFunctionClass.getAnnotation(EvaluationFunction.class);
        assert evalAnnotation != null;
        name = evalAnnotation.evaluationName();

        // getValue the output data type of the function class
        outputDataType = evalAnnotation.dataType();

        /*
         * literal binding
         */
        // bind the holders of the annotated eval function and extract literal parameters
        List<Field> literalFields = AnnotationUtils.getValuesFromAnnotatedType(annotatedEvalFunctionInstance, ParameterValue.class);
        literalFields.forEach(literalField -> {
            ParameterValue annotation = literalField.getAnnotation(ParameterValue.class);
            Holder literalHolder = new Holder(null, annotation.dataType().getClassType());
            literalValueHolder.put(annotation.name(), literalHolder);
            literalTypeHolder.put(annotation.name(), annotation.dataType());

            ParameterValue channelValue = (ParameterValue) Arrays.stream(literalField.getAnnotations()).filter(a -> a instanceof ParameterValue).findFirst().get();
            parameters.add(new FunctionParameter(channelValue.name(), FunctionParameterType.LITERAL, channelValue.dataType()));
        });
        // getValue parameter fields
        List<Field> fields = AnnotationUtils.findFields(annotatedEvalFunctionInstance.getClass(), ParameterValue.class);
        for (int i = 0; i < Math.min(fields.size(), literalFields.size()); i++) {
            Field field = fields.get(i);
            ParameterValue parameterValue = field.getAnnotation(ParameterValue.class);
            fields.get(i).setAccessible(true);
            field.set(annotatedEvalFunctionInstance, literalValueHolder.get(parameterValue.name()));
        }

        /*
         * channel binding
         */
        List<Field> channelFields = AnnotationUtils.getValuesFromAnnotatedType(annotatedEvalFunctionInstance, ChannelValue.class);
        channelFields.forEach(channelField -> {
            ChannelValue annotation = channelField.getAnnotation(ChannelValue.class);
            Holder inChannelAndFunctionHolder = new Holder(null, annotation.dataType().getClassType());
            channelValueHolder.put(annotation.name(), inChannelAndFunctionHolder);
            channelTypeHolder.put(annotation.name(), annotation.dataType());

            ChannelValue channelValue = (ChannelValue) Arrays.stream(channelField.getAnnotations()).filter(a -> a instanceof ChannelValue).findFirst().get();
            parameters.add(new FunctionParameter(channelValue.name(), FunctionParameterType.CHANNEL, channelValue.dataType()));
        });
        // set channel fields
        List<Field> fields2 = AnnotationUtils.findFields(annotatedEvalFunctionInstance.getClass(), ChannelValue.class);
        for (int i = 0; i < Math.min(fields2.size(), channelFields.size()); i++) {
            Field field = fields2.get(i);
            ChannelValue channelValue = field.getAnnotation(ChannelValue.class);
            fields2.get(i).setAccessible(true);
            field.set(annotatedEvalFunctionInstance, channelValueHolder.get(channelValue.name()));
        }


        // TimeValueAnnotation
        List<Field> timeFields = AnnotationUtils.getValuesFromAnnotatedType(annotatedEvalFunctionInstance, TimeValue.class);
        timeValueHolder = new Holder(null, Long.class);
        for (Field timeField : timeFields) {
            timeField.setAccessible(true);
            timeField.set(annotatedEvalFunctionInstance, timeValueHolder);
        }

        List<Field> resultTypeFields = AnnotationUtils.getValuesFromAnnotatedType(annotatedEvalFunctionInstance, ResultTypes.class);
        resultTypeFields.forEach(resultTypeField -> {
            ResultTypes resultTypes = (ResultTypes) Arrays.stream(resultTypeField.getAnnotations()).filter(r -> r instanceof ResultTypes).findFirst().get();
            functionResults = new FunctionResults(resultTypes.resultTypes());
        });
    }

    /**
     * Generates a {@link FunctionDef} for the {@link AnnotatedEvalFunction}
     *
     * @return a {@link FunctionDef} of the {@link AnnotatedEvalFunction}
     */
    @Override
    public FunctionDef getFunctionDef() {

        List<FunctionParameter> localParameters = this.parameters.stream().map(parameter -> {
            FunctionParameterType parameterType = parameter.getParameterType();
            return new FunctionParameter(parameter.getName(), parameterType, parameter.getDataType());
        }).collect(Collectors.toList());

        return new FunctionDef(new Signature(name, localParameters), outputDataType, annotatedEvalFunctionClass, functionResults);
    }

    /**
     * Method that is called before processing. It initializes the {@link EvalFunction} structures.
     * In this case of {@link AnnotatedEvalFunction}, the setup parameters are injected in the object structures of the
     * wrapped object.
     *
     * @param literals     Literal parameters of the function
     * @param eventHandler Handles the messaging of results of the {@link EvalFunction}
     */
    @Override
    public void setup(Map<String, Value> literals, EventHandler eventHandler) {

        // inject result handler into the function
        AnnotationUtils.injectEventStream(annotatedEvalFunctionInstance, eventHandler);

        // set literals
        literalValueHolder.forEach((key, value) -> value.set(literals.get(key)
                .getAsDataType(literalTypeHolder.get(key))
        ));

        // call setup in annotated eval function
        annotatedEvalFunctionInstance.setup();
    }

    /**
     * Method is called for processing of each record
     *
     * @param channels of the record to be processed
     * @return output value of the function
     */
    @Override
    public T eval(long timestamp, Map<String, Value> channels) {
        // set channel holder values
        channelValueHolder.forEach((key, value) -> {
            if (!channels.containsKey(key)) {
                Optional<String> reduce = channels.keySet().stream().reduce((a, b) -> a + ", " + b);
                throw new IllegalStateException(String.format(
                        "Trying to inject channel \"%s\" but is not present in the data! Available channel names: %s",
                        key,
                        reduce.orElse("none")
                ));
            }
            value.set(channels.get(key).getAsDataType(channelTypeHolder.get(key)));
        });

        timeValueHolder.set(timestamp);


        // call eval in annotated eval function
        return annotatedEvalFunctionInstance.eval();
    }

    /**
     * Method is called when processing is finished to clean up the {@link EvalFunction} and to send end results
     */
    @Override
    public void finish() {
        annotatedEvalFunctionInstance.finish();
    }

    public static class AnnotatedEvalFunctionWrapperFactory<T> implements EvalFunctionFactory, Serializable {

        private Class<? extends AnnotatedEvalFunction<T>> clazz;

        public AnnotatedEvalFunctionWrapperFactory(Class<? extends AnnotatedEvalFunction<T>> clazz) {
            this.clazz = clazz;
        }

        @Override
        public AnnotatedEvalFunctionWrapper<T> create() throws IllegalAccessException {
            return new AnnotatedEvalFunctionWrapper<>(clazz);
        }
    }
}
