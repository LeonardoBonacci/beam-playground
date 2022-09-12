package org.apache.beam.stream;

import org.apache.beam.help.PrintFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class KafkaSlidingWindows {

  private static final Duration ONE_SECOND = Duration.standardSeconds(1);
  private static final Duration FIVE_SECONDS = Duration.standardSeconds(5);

  public static void main(String[] args) {
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> fromKafka = 
      p.apply(KafkaIO.<String, String>read()
       .withBootstrapServers("localhost:9092")
       .withTopic("foo") 
       .withKeyDeserializer(StringDeserializer.class)
       .withValueDeserializer(StringDeserializer.class)
       .withoutMetadata()
    )
    .apply(Values.<String>create());
    
    PCollection<String> windowed_items = fromKafka.apply(
        Window.<String>into(SlidingWindows.of(FIVE_SECONDS).every(ONE_SECOND))
           .triggering(
               AfterWatermark.pastEndOfWindow()
                   .withLateFirings(AfterProcessingTime
                       .pastFirstElementInPane().plusDelayOf(ONE_SECOND)))
           .withAllowedLateness(ONE_SECOND)
           .discardingFiredPanes());
      
    PCollection<KV<String, Long>> windowed_counts = windowed_items.apply(
      Count.<String>perElement());
    
    windowed_counts
      .apply(
        MapElements.into(TypeDescriptors.strings())
          .via(
              (KV<String, Long> wordC) -> 
                wordC.getKey() + "<>" + wordC.getValue()))
      .apply(ParDo.of(new PrintFn()));
    
    p.run().waitUntilFinish();
  }
}
