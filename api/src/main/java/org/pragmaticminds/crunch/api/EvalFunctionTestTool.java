package org.pragmaticminds.crunch.api;

import org.pragmaticminds.crunch.api.events.EventHandler;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to test {@link EvalFunction} classes for their functionality
 * <p>
 * Created by Erwin Wagasow on 22.06.2017.
 *
 * @deprecated Part of the old API
 */
@Deprecated
@SuppressWarnings("squid:S1612") // Use Lambdas for Handler, no method references
public class EvalFunctionTestTool {

    private EventHandler eventHandler;
    private EvaluationTestToolEvents events;
    private EvalFunction evalFunction;

    /**
     * Constructor which works with {@link EvalFunction} class
     *
     * @param evalFunctionClass class of the {@link EvalFunction}
     * @throws IllegalAccessException occurs on access to private members
     * @throws InstantiationException occurs on failing of creation of new instances of the given class
     */
    public EvalFunctionTestTool(Class<? extends EvalFunction> evalFunctionClass) throws IllegalAccessException,
            InstantiationException {
        this.eventHandler = event -> events.addEvent(event);

        // create instance of the evaluation class
        evalFunction = evalFunctionClass.newInstance();
    }

    public EvalFunctionTestTool(EvalFunction evalFunction) {
        this.eventHandler = event -> events.addEvent(event);

        // create instance of the evaluation class
        this.evalFunction = evalFunction;
    }

    /**
     * By calling this method the evaluation is started with the given parameters
     *
     * @param literals   constant values of the evaluation function
     * @param channels   time stream values of the evaluation function
     * @param timestamps time indices of the time stream (channels) values
     * @return an Object containing all outputs and results of the execution
     */
    public EvaluationTestToolEvents execute(Map<String, Value> literals, List<Map<String, Value>> channels, List<Long> timestamps) {
        events = new EvaluationTestToolEvents();

        // Phase 1
        evalFunction.setup(literals, eventHandler);

        // Phase 2
        // eval with channel values
        boolean collectionsHaveSameSize = channels.size() == timestamps.size();
        assert collectionsHaveSameSize;

        for (int i = 0; i < channels.size(); i++) {
            Object output = evalFunction.eval(timestamps.get(i), channels.get(i));
            this.events.addOutput(output);
        }

        // Phase 3
        evalFunction.finish();

        return events;
    }

    /**
     * The Object which contains all results of a EvaluationFunction execution.
     * A list of results and a list of outputs.
     */
    public static class EvaluationTestToolEvents {
        private List<Event> events = new ArrayList<>();
        private List<Object> outputs = new ArrayList<>();

        public List<Event> getEvents() {
            return events;
        }

        public void setEvents(List<Event> events) {
            this.events = events;
        }

        public List<Object> getOutputs() {
            return outputs;
        }

        public void setOutputs(List<Object> outputs) {
            this.outputs = outputs;
        }

        void addEvent(Event event) {
            this.events.add(event);
        }

        public void addEvents(List<Event> events) {
            this.events.addAll(events);
        }

        void addOutput(Object output) {
            this.outputs.add(output);
        }

        public void addOutputs(List<Object> outputs) {
            this.outputs.addAll(outputs);
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append(super.toString());
            stringBuilder.append(": \n");

            stringBuilder.append("function outputs: \n");
            for (Object o : this.getOutputs()) {
                stringBuilder.append(o).append("\n");
            }
            stringBuilder.append("function events: \n");
            for (Event event : this.getEvents()) {
                stringBuilder.append(event);
            }

            return stringBuilder.toString();
        }
    }
}
