package org.pragmaticminds.crunch.execution;

import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.EvaluationPipeline;
import org.pragmaticminds.crunch.api.pipe.SubStream;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

/**
 * "Integration" test to check {@link GraphFactory} and {@link CrunchExecutor}.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class CrunchExecutorTest {

    /**
     * Creates a Simple Pipeline and runs it.
     * And checks that 2 Events are emitted.
     */
    @Test
    public void run() {
        // Create source
        UntypedValues values = UntypedValues.builder()
            .source("test")
            .prefix("")
            .timestamp(123L)
            .values(Collections.singletonMap("test", "test"))
            .build();
        
        MRecordSource source = MRecordSources.of(values, values);
        // Create Pipeline
        EvaluationPipeline pipeline = createPipeline();
        // Create Sink
        EventSink sink = Mockito.mock(EventSink.class);
        // Create the Executor
        CrunchExecutor crunchExecutor = new CrunchExecutor(source, pipeline, sink);
        // Run the executor
        crunchExecutor.run();
        // Ensure that two events have been reported
        Mockito.verify(sink, times(2)).handle(any());
    }

    private EvaluationPipeline createPipeline() {
        return EvaluationPipeline.builder()
                .withIdentifier("bsdf")
                .withSubStream(
                        SubStream.builder()
                                .withIdentifier("asdf")
                                .withPredicate(x -> true)
                                .withEvaluationFunction(new MyEvaluationFunction())
                                .build()
                )
                .build();
    }

    private static class MyEvaluationFunction implements EvaluationFunction {
        @Override
        public void eval(EvaluationContext ctx) {
            ctx.collect(GenericEventBuilder.anEvent()
                    .withEvent("success")
                    .withTimestamp(0L)
                    .withSource("no source")
                    .build());
        }
    
        /**
         * Collects all channel identifiers, that are used for the triggering condition
         *
         * @return a {@link List} or {@link Collection} of all channel identifiers from triggering
         */
        @Override
        public Set<String> getChannelIdentifiers() {
            return null;
        }
    }
}