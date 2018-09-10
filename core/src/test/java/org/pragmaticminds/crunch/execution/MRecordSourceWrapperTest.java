package org.pragmaticminds.crunch.execution;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Procedure;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;
import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * @author julian
 * Created by julian on 10.09.18
 */
public class MRecordSourceWrapperTest {

    private static final MRecord DUMMY_RECORD = new MRecord() {
        @Override
        public String getSource() {
            return null;
        }

        @Override
        public Double getDouble(String channel) {
            return null;
        }

        @Override
        public Long getLong(String channel) {
            return null;
        }

        @Override
        public Boolean getBoolean(String channel) {
            return null;
        }

        @Override
        public Date getDate(String channel) {
            return null;
        }

        @Override
        public String getString(String channel) {
            return null;
        }

        @Override
        public Value getValue(String channel) {
            return null;
        }

        @Override
        public Object get(String channel) {
            return null;
        }

        @Override
        public Collection<String> getChannels() {
            return null;
        }

        @Override
        public long getTimestamp() {
            return 0;
        }
    };

    /**
     * See BUG CRUNCH-570
     */
    @Test
    public void wrapperEmitsNull_sourceNotAllowedToThrowException() throws ExecutionException, InterruptedException {
        MRecordSourceWrapper wrapper = new MRecordSourceWrapper(getSource());

        // Try to run it using Akka Streams
        ActorSystem system = ActorSystem.create("CrunchExecutor");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        Source<MRecord, NotUsed> streamSource = Source.fromGraph(wrapper);

        // Run it and take the first element
        AtomicReference<MRecord> reference = new AtomicReference<>(null);
        CompletionStage<Done> stage = streamSource.take(1).runForeach(new Procedure<MRecord>() {
            @Override
            public void apply(MRecord param) throws Exception {
                reference.set(param);
            }
        }, materializer);

        // Wait to finish
        stage.toCompletableFuture().get();

        // Check if the reference is the null object
        assertEquals(DUMMY_RECORD, reference.get());
    }

    /**
     * Returns a source that returns 100 null values and then one dummy record.
     *
     * @return Source
     */
    private MRecordSource getSource() {
        return new MRecordSource() {

            private long counter = 0;

            @Override
            public MRecord get() {
                return (counter++ < 100) ? null : DUMMY_RECORD;
            }

            @Override
            public boolean hasRemaining() {
                return (counter < 100);
            }

            @Override
            public void init() {

            }

            @Override
            public void close() {

            }

            @Override
            public Kind getKind() {
                return Kind.UNKNOWN;
            }
        };
    }

}