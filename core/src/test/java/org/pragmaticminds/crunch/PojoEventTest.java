package org.pragmaticminds.crunch;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.*;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.execution.CrunchExecutor;
import org.pragmaticminds.crunch.execution.EventSink;
import org.pragmaticminds.crunch.execution.MRecordSources;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the feature CRUNCH-577 that PoJos can be used as Events.
 * This is just an Example on how to use this feature.
 *
 * @author julian
 * Created by julian on 02.10.18
 */
public class PojoEventTest implements Serializable {

    @Test
    public void usePojoAsEventCrunchExecutor() throws InterruptedException {
        EvaluationPipeline<MyPojo> pipeline = createPipeline();

        CrunchExecutor executor = new CrunchExecutor(MRecordSources.of(
                UntypedValues.builder()
                        .source("")
                        .timestamp(1L)
                        .prefix("")
                        .values(Collections.singletonMap("key", 1L))
                        .build()
        ), pipeline);

        executor.run(new PojoEventSink());

        TimeUnit.MILLISECONDS.sleep(1_000);
    }

    private EvaluationPipeline<MyPojo> createPipeline() {
        EvaluationFunction<MyPojo> function = new EvaluationFunction<MyPojo>() {
            @Override
            public void eval(EvaluationContext<MyPojo> ctx) {
                ctx.collect(new MyPojo("Hello from the eval function"));
            }

            /**
             * Returns all channel identifiers which are necessary for the function to do its job.
             * It is not allowed to return null, an empty set can be returned (but why should??).
             *
             * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
             */
            @Override
            public Set<String> getChannelIdentifiers() {
                return Collections.emptySet();
            }
        };

        SubStream<MyPojo> stream = SubStream.<MyPojo>builder()
                .withIdentifier("id")
                .withPredicate((SubStreamPredicate) values -> true)
                .withEvaluationFunction(function)
                .build();

        return EvaluationPipeline.<MyPojo>builder()
                .withIdentifier("my pipeline")
                .withSubStream(stream)
                .build();
    }

    private static class MyPojo implements Serializable {

        private final String value;

        public MyPojo(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "MyPojo{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    private static class PojoEventSink implements EventSink<MyPojo> {

        @Override
        public void handle(MyPojo event) {
            System.out.println("Value of the Pojo: " + event.getValue());
        }
    }
}
