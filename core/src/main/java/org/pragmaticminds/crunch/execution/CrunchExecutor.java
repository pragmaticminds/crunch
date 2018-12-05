/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.execution;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.RunnableGraph;
import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.EvaluationPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/**
 * Executor that runs a {@link org.pragmaticminds.crunch.api.pipe.EvaluationPipeline}.
 * After the Executor is constructed one can simply invoke one of the {@link #run()} methods to run the Pipeline.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class CrunchExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CrunchExecutor.class);

    /**
     * Use one actor system for all executors in one jvm.
     */
    private static final ActorSystem SYSTEM = ActorSystem.create("CrunchExecutor");

    /**
     * Use one Fraph Factory
     */
    private static final GraphFactory GRAPH_FACTORY = new GraphFactory();

    private MRecordSource source;
    private EvaluationPipeline evaluationPipeline;
    private EventSink sink;

    public CrunchExecutor(MRecordSource source, EvaluationPipeline evaluationPipeline, EventSink sink) {
        this.source = source;
        this.evaluationPipeline = evaluationPipeline;
        this.sink = sink;
    }

    public CrunchExecutor(MRecordSource source, EvaluationPipeline evaluationPipeline) {
        this(source, evaluationPipeline, null);
    }

    /**
     * Runs the Pipeline using the Sink given in the Constructor.
     * Blocks white the Pipeline is running.
     */
    public void run() {
        runWithSink(this.sink);
    }

    /**
     * Runs the Pipeline using the Sink given in the Argument
     * Blocks white the Pipeline is running.
     *
     * @param sink Sink to use for Events
     */
    public void run(EventSink sink) {
        runWithSink(sink);
    }

    /**
     * Internal method to run the Pipeline with a given Sink.
     *
     * @param sink Sink to use
     */
    private void runWithSink(EventSink sink) {
        Preconditions.checkNotNull(sink, "Please provide a Sink!");
        Materializer materializer = ActorMaterializer.create(SYSTEM);
        Long watermarkOffsetMs = 50L;

        RunnableGraph<CompletionStage<Done>> runnableGraph = GRAPH_FACTORY.create(source, evaluationPipeline, sink, watermarkOffsetMs);

        try {
            runnableGraph.run(materializer).toCompletableFuture().get();
        } catch (ExecutionException e) {
            logger.warn("Unable to wait for execution of pipeline.", e);
        } catch (InterruptedException e){
            logger.warn("Unable to wait for execution of pipeline.", e);
            Thread.currentThread().interrupt();
        }
    }

}
