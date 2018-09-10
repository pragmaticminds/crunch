package org.pragmaticminds.crunch.execution;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around the {@link MRecordSource} as Akka {@link GraphStage}.
 * It takes a {@link MRecordSource} and acts then as {@link GraphStage} which is the source
 * in an Akka Stream.
 *
 * ATTENTION: This code can be red (although it compiles) in IntelliJ due to a Bug
 *
 * @author julian
 * Created by julian on 15.08.18
 */
class MRecordSourceWrapper extends GraphStage<SourceShape<MRecord>> {

    private static final Logger logger = LoggerFactory.getLogger(MRecordSourceWrapper.class);

    // Define the (sole) output port of this stage
    public final Outlet<MRecord> out = Outlet.create("MRecordSource.out");

    // Define the shape of this stage, which is SourceShape with the port we defined above
    private final SourceShape<MRecord> shape = SourceShape.of(out);

    private final MRecordSource source;

    public MRecordSourceWrapper(MRecordSource source) {
        this.source = source;
    }

    @Override
    public SourceShape<MRecord> shape() {
        return shape;
    }

    @Override
    @SuppressWarnings({"squid:S1171", "squid:S1188"}) // Use Init Block and keep it inline ( to stick to Akka Standards)
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {
            // All state MUST be inside the GraphStageLogic,
            // never inside the enclosing GraphStage.
            // This state is safe to access and modify from all the
            // callbacks that are provided by GraphStageLogic and the
            // registered handlers.

            // Initialization in Akka Java is usually done in static init blocks
            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if (!source.hasRemaining()) {
                            complete(out);
                        } else {
                            // If the underlying source returned null we pull again
                            MRecord record;
                            while ((record = source.get()) == null) {
                                logger.warn("Skipped Null Record from source, fetching next record");
                            }
                            push(out, record);
                        }
                    }
                });
            }

            @Override
            public void preStart() throws Exception {
                logger.info("Initializing stream source {} with Cardinality {}", source, source.getKind());
                source.init();
            }

            @Override
            public void postStop() throws Exception {
                logger.info("Closing stream source {}", source);
                source.close();
            }

        };
    }
}
