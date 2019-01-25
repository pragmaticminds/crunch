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

package org.pragmaticminds.chronicle;

import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.EvaluationPipeline;
import org.pragmaticminds.crunch.api.pipe.SubStream;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.trigger.extractor.Extractors;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.trigger.handler.GenericExtractorTriggerHandler;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.execution.AbstractMRecordSource;
import org.pragmaticminds.crunch.execution.CrunchExecutor;
import org.pragmaticminds.crunch.execution.EventSink;
import org.pragmaticminds.crunch.execution.MRecordSource;
import org.pragmaticminds.crunch.source.FileMRecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.*;
import static org.pragmaticminds.crunch.api.trigger.filter.EventFilters.onValueChanged;
import static org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategies.onBecomeTrue;

/**
 * @author kerstin
 * Created by kerstin on 16.01.18
 */
public class TestCrunchPerformanceNT {

    static ArrayList<SubStream<GenericEvent>> getSubStreams() {
        List<String> machines = Arrays.asList("LHL_01", "LHL_02", "LHL_03", "LHL_04", "LHL_05", "LHL_06");
        List<String> mixers = Arrays.asList("Mixer01", "Mixer02");


        ArrayList<SubStream<GenericEvent>> subStreams = new ArrayList<>();

      for (String mixer : mixers) {
            subStreams.add(
                    SubStream.<GenericEvent>builder()
                            // set the name of the SubStream
                        .withIdentifier(mixer)

                            // set the sort window queue
                            .withSortWindow(100L)

                            // only pass MRecords where the source is the same as machineName
                        .withPredicate(x -> x.getSource().equals(mixer))

//                            .withEvaluationFunction(new EvaluationFunction<GenericEvent>() {
//                              @Override public void eval(EvaluationContext<GenericEvent> ctx) {
//                                // do nothing
//                              }
//
//                              @Override public Set<String> getChannelIdentifiers() {
//                                return Collections.singleton(DB401_XDATA + 1 + "]_PAR_PV_XSD10kg");
//                              }
//                            })

                            // NEW_COMPLETE_CYCLE function
                        .withEvaluationFunctions(getPipelinesForMixers())

                            .build());
        }

//        for (String mixerName : mixers) {
//            subStreams.add(
//                    SubStream.<GenericEvent>builder()
//                            // set sub stream name
//                            .withIdentifier(mixerName)
//
//                            // set the sort window queue
//                            .withSortWindow(100)
//
//                            // only pass MRecords where the source starts with "mixer"
//                            .withPredicate(x -> x.getSource().equals(mixerName))
//
//                            // set the evaluation functions
//                            .withEvaluationFunctions(IntStream.rangeClosed(1, 6)
//                                    .mapToObj(
//                                            MixerMixtureDetectionFactory::create
//                                    )
//                                    // collect the created TriggerEvaluationFunctions
//                                    .collect(Collectors.toList()) // return in the top
//                            )
//                            .build());
//        }
        return subStreams;
    }

  @NotNull private static List<EvaluationFunction<GenericEvent>> getPipelinesForMixers() {
    return IntStream.rangeClosed(1, 6).mapToObj(MixerMixtureDetectionFactory::create).collect(Collectors.toList());
  }

  public static void main(String[] args) {
    new TestCrunchPerformanceNT().run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 2)
  public void run() {

        EvaluationPipeline<GenericEvent> eventEvaluationPipeline = EvaluationPipeline.<GenericEvent>builder().withIdentifier("CRUNCH_TEST").withSubStreams(getSubStreams()).build();

    MRecordSource kafkaMRecordSource = new UpdatingSource(new FileMRecordSource("/tmp/sample2.txt"));
        List<GenericEvent> events = new ArrayList<>();

        CrunchExecutor crunchExecutor = new CrunchExecutor(kafkaMRecordSource, eventEvaluationPipeline,
                new EventSink<GenericEvent>() {

                    @Override
                    public void handle(GenericEvent genericEvent) {
                      System.out.println(genericEvent);
                    }
                }
        );
        crunchExecutor.run();
    }

  public static class UpdatingSource extends AbstractMRecordSource {

    private static final Logger logger = LoggerFactory.getLogger(UpdatingSource.class);

    private MRecordSource source;

    private Map<String, TimestampedValue> lastValues = new HashMap<>();

    public UpdatingSource(MRecordSource source) {
      super(source.getKind());
      this.source = source;
    }

    @Override
    public MRecord get() {
      MRecord record = source.get();

      if (record == null) {
        return null;
      }

      Set<String> obsolete = record.getChannels().stream()
              .filter(cn -> lastValues.containsKey(cn))
              .filter(cn -> lastValues.get(cn).value.equals(record.getValue(cn)))
              .filter(cn -> record.getTimestamp() - lastValues.get(cn).timestamp <= 60_000)
              .collect(Collectors.toSet());

      // System.out.println(record.getChannels());
      System.out.println(obsolete);

      // Remove obsolete channels
      HashMap<String, Object> remainingValues = new HashMap<>();
      for (String channel : record.getChannels()) {
        if (!obsolete.contains(channel)) {
          // Store and Emit
          lastValues.put(channel,
                  new TimestampedValue(record.getTimestamp(), record.getValue(channel)));
          remainingValues.put(channel, record.getValue(channel));
//            logger.info("Keeping channel...");
        }
//          logger.info("Dropping channel...");
      }
      return new UntypedValues(record.getSource(), record.getTimestamp(), "", remainingValues);
    }


    @Override
    public boolean hasRemaining() {
      return source.hasRemaining();
    }

  }

  public static class TimestampedValue {

    public final long timestamp;
    public final Object value;

    public TimestampedValue(long timestamp, Object value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TimestampedValue that = (TimestampedValue) o;
      return Objects.equals(timestamp, that.timestamp) &&
              Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, value);
    }

    @Override
    public String toString() {
      return "TimestampedValue{" +
              "timestamp=" + timestamp +
              ", value=" + value +
              '}';
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  public void hashMapBenchmark() {
    final HashMap<Object, Object> map = new HashMap<>();
    for (int i = 1; i <= 300 * 50 * 2; i++) {
      map.put(UUID.randomUUID().toString(), 1L);
    }
  }

  /**
   * This factory creates Mixer-Mixture-Detection {@link EvaluationFunction}s in the new fashion way.
   * The base for definition of the {@link EvaluationFunction} is the {@link TriggerEvaluationFunction}.
   *
   * @author Erwin Wagasow
   * Created by Erwin Wagasow on 10.09.2018
   */
  public static class MixerMixtureDetectionFactory {

    public static final String DB401_XDATA = "DB401_XDATA[";

    /** hidden constructor */
    private MixerMixtureDetectionFactory() {
      throw new UnsupportedOperationException("this constructor should not be used!");
    }

    /**
     * Implements the structures of mixer mixture detection by parametrising a {@link TriggerEvaluationFunction}.
     *
     * @param machineId of the machine of interest.
     * @return parametrised instance of a {@link TriggerEvaluationFunction}.
     */
    public static EvaluationFunction<GenericEvent> create(int machineId) {
      return TriggerEvaluationFunction.<GenericEvent>builder()
          // create the TriggerStrategy with the decision rules how to handle trigger signals
          .withTriggerStrategy(
              onBecomeTrue(
                  booleanChannel(DB401_XDATA + machineId + "]_CT_S_Mischen_fertig")
              )
          )
          // create the EventExtractor which extracts Events from the current messages
          .withTriggerHandler(new GenericExtractorTriggerHandler("Mix_Finished", createMappings(machineId)))
          // construct the TriggerEvaluationFunction
          .withFilter(
              onValueChanged(
                  longChannel(DB401_XDATA + machineId + "]_CT_S_Mischungsnummer")
              )
          )
          .build();
    }

    /**
     * Creates the mappings from channel names to target parameter names in the resulting event
     *
     * @param machineId of interest
     * @return mappings from channel names to event parameter names
     */
    private static List<MapExtractor> createMappings(int machineId) {

      Map<Supplier, String> channelMappings = new HashMap<>();

      channelMappings.put(channel(DB401_XDATA + machineId + "]_CT_D_Maschine"), "asdf");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_CT_D_Rezeptnummer"), "asdf1");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_CT_S_Mischen_fertig"), "asdf2");
      // time parameters actual
      channelMappings.put(channel(DB401_XDATA + machineId + "]_RC_PV_Vormischzeit"), "asdf3");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_RC_PV_Mischzeit"), "asdf4");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_RC_PV_Entleerzeit"), "asdf5");
      // time parameters target
      channelMappings.put(channel(DB401_XDATA + machineId + "]_RC_SP_Vormischzeit"), "asdf6");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_RC_SP_Mischzeit"), "asdf7");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_RC_SP_Entleerzeit"), "asdf8");
      // weight parameters actual values
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XSD10kg"), "asdf9");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XSD11kg"), "asdf10");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XSD12kg"), "asdf11");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XSD13kg"), "asdf12");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XTU10g"), "asdf13");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XTU20g"), "asdf14");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XTU30g"), "asdf15");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_XTU40g"), "asdf16");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_YAD10g"), "asdf17");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_YAD20g"), "asdf18");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_YAD30g"), "asdf19");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_PV_YAD40g"), "asdf20");
      // weight parameters target values
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XSD10kg"), "asdf21");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XSD11kg"), "asdf22");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XSD12kg"), "asdf23");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XSD13kg"), "asdf24");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XTU10g"), "asdf25");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XTU20g"), "asdf26");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XTU30g"), "asdf27");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_XTU40g"), "asdf28");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_YAD10g"), "asdf29");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_YAD20g"), "asdf30");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_YAD30g"), "asdf31");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_PAR_SP_YAD40g"), "asdf32");
      channelMappings.put(channel(DB401_XDATA + machineId + "]_CT_S_Mischungsnummer"), "asdf33");

      List<MapExtractor> mapExtractorList = new ArrayList<>();
      mapExtractorList.add(Extractors.channelMapExtractor(channelMappings));

      mapExtractorList.add(new LocalMapExtractor(machineId));
      return mapExtractorList;
    }

    /**
     * Extracts the calculated values for the resulting GenericEvent
     */
    private static class LocalMapExtractor implements MapExtractor {

      private int machineId;

      /**
       * Main constructor, that takes the machineId as parameter.
       *
       * @param machineId of interest.
       */
      LocalMapExtractor(int machineId) {
        this.machineId = machineId;
      }

      /**
       * This method extracts a map of {@link Value}s from a {@link EvaluationContext}, in particular from it's
       * {@link MRecord}.
       *
       * @param context the current {@link EvaluationContext} that holds the current {@link MRecord}.
       * @return a {@link Map} of keyed extracted values from the {@link EvaluationContext}s {@link MRecord}.
       */
      @Override
      public Map<String, Value> extract(EvaluationContext context) {

        // calculate the dry mass
        double gramPerKilogram = 1000.0;
        double millisecondsPerSecond = 1000.0;

        // calculate the dry mass
//        double dryMass = context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_XSD10kg") +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_XTU10g") / gramPerKilogram +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_XTU20g") / gramPerKilogram +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_XTU30g") / gramPerKilogram +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_XTU40g") / gramPerKilogram;
//
//        // calculate the complete mass
//        double completeMass = dryMass +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_YAD10g") / gramPerKilogram +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_YAD20g") / gramPerKilogram +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_YAD30g") / gramPerKilogram +
//            context.get().getDouble(DB401_XDATA + machineId + "]_PAR_PV_YAD40g") / gramPerKilogram;
//
//        // calculate complete mix time ACTUAL
//        double completeMixTimeActual = context.get().getDouble(DB401_XDATA + machineId + "]_RC_PV_Vormischzeit") / millisecondsPerSecond +
//            context.get().getDouble(DB401_XDATA + machineId + "]_RC_PV_Mischzeit") / millisecondsPerSecond +
//            context.get().getDouble(DB401_XDATA + machineId + "]_RC_PV_Entleerzeit") / millisecondsPerSecond;
//
//        // calculate complete mix time TARGET
//        double completeMixTimeTarget = context.get().getDouble(DB401_XDATA + machineId + "]_RC_SP_Vormischzeit") / millisecondsPerSecond +
//            context.get().getDouble(DB401_XDATA + machineId + "]_RC_SP_Mischzeit") / millisecondsPerSecond +
//            context.get().getDouble(DB401_XDATA + machineId + "]_RC_SP_Entleerzeit") / millisecondsPerSecond;

        Map<String, Value> mappings = new HashMap<>();
//        mappings.put("bsdf1", Value.of(dryMass));
//        mappings.put("bsdf2", Value.of(completeMass));
//        mappings.put("bsdf3", Value.of(completeMixTimeTarget));
//        mappings.put("bsdf4", Value.of(completeMixTimeActual));
        return mappings;
      }

      @Override public Set<String> getChannelIdentifiers() {
        return new HashSet<>(Arrays.asList(
//            DB401_XDATA + machineId + "]_PAR_PV_XSD10kg",
//            DB401_XDATA + machineId + "]_PAR_PV_XTU10g",
//            DB401_XDATA + machineId + "]_PAR_PV_XTU20g",
//            DB401_XDATA + machineId + "]_PAR_PV_XTU30g",
//            DB401_XDATA + machineId + "]_PAR_PV_XTU40g",
//            DB401_XDATA + machineId + "]_PAR_PV_YAD10g",
//            DB401_XDATA + machineId + "]_PAR_PV_YAD20g",
//            DB401_XDATA + machineId + "]_PAR_PV_YAD30g",
//            DB401_XDATA + machineId + "]_PAR_PV_YAD40g",
//            DB401_XDATA + machineId + "]_RC_PV_Vormischzeit",
//            DB401_XDATA + machineId + "]_RC_PV_Mischzeit",
//            DB401_XDATA + machineId + "]_RC_PV_Entleerzeit",
//            DB401_XDATA + machineId + "]_RC_SP_Vormischzeit",
//            DB401_XDATA + machineId + "]_RC_SP_Mischzeit",
//            DB401_XDATA + machineId + "]_RC_SP_Entleerzeit",
//            DB401_XDATA + machineId + "]_CT_D_Maschine",
//            DB401_XDATA + machineId + "]_CT_D_Rezeptnummer",
            DB401_XDATA + machineId + "]_CT_S_Mischen_fertig",
            // time parameters actual
            DB401_XDATA + machineId + "]_RC_PV_Vormischzeit",
            DB401_XDATA + machineId + "]_RC_PV_Mischzeit",
            DB401_XDATA + machineId + "]_RC_PV_Entleerzeit",
            // time parameters target
            DB401_XDATA + machineId + "]_RC_SP_Vormischzeit",
            DB401_XDATA + machineId + "]_RC_SP_Mischzeit",
            DB401_XDATA + machineId + "]_RC_SP_Entleerzeit",
            // weight parameters actual values
            DB401_XDATA + machineId + "]_PAR_PV_XSD10kg",
            DB401_XDATA + machineId + "]_PAR_PV_XSD11kg",
            DB401_XDATA + machineId + "]_PAR_PV_XSD12kg",
            DB401_XDATA + machineId + "]_PAR_PV_XSD13kg",
            DB401_XDATA + machineId + "]_PAR_PV_XTU10g",
            DB401_XDATA + machineId + "]_PAR_PV_XTU20g",
            DB401_XDATA + machineId + "]_PAR_PV_XTU30g",
            DB401_XDATA + machineId + "]_PAR_PV_XTU40g",
            DB401_XDATA + machineId + "]_PAR_PV_YAD10g",
            DB401_XDATA + machineId + "]_PAR_PV_YAD20g",
            DB401_XDATA + machineId + "]_PAR_PV_YAD30g",
            DB401_XDATA + machineId + "]_PAR_PV_YAD40g",
            // weight parameters target values
            DB401_XDATA + machineId + "]_PAR_SP_XSD10kg",
            DB401_XDATA + machineId + "]_PAR_SP_XSD11kg",
            DB401_XDATA + machineId + "]_PAR_SP_XSD12kg",
            DB401_XDATA + machineId + "]_PAR_SP_XSD13kg",
            DB401_XDATA + machineId + "]_PAR_SP_XTU10g",
            DB401_XDATA + machineId + "]_PAR_SP_XTU20g",
            DB401_XDATA + machineId + "]_PAR_SP_XTU30g",
            DB401_XDATA + machineId + "]_PAR_SP_XTU40g",
            DB401_XDATA + machineId + "]_PAR_SP_YAD10g",
            DB401_XDATA + machineId + "]_PAR_SP_YAD20g",
            DB401_XDATA + machineId + "]_PAR_SP_YAD30g",
            DB401_XDATA + machineId + "]_PAR_SP_YAD40g",
            DB401_XDATA + machineId + "]_CT_S_Mischungsnummer"
        ));
      }
    }
  }

}
