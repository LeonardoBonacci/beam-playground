package org.apache.beam.stream;

import org.apache.beam.help.PrintFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaRead {

  public static void main(String[] args) {
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, String>> fromKafka = 
      p.apply(KafkaIO.<String, String>read()
       .withBootstrapServers("localhost:9092")
       .withTopic("foo") 
       .withKeyDeserializer(StringDeserializer.class)
       .withValueDeserializer(StringDeserializer.class)
       .withoutMetadata() 
    );
    
    fromKafka
      .apply(
        MapElements.into(TypeDescriptors.strings())
          .via(
              (KV<String, String> kv) -> 
                kv.getKey() + "<>" + kv.getValue()))
    .apply(ParDo.of(new PrintFn()));
    
    p.run().waitUntilFinish();
  }
}
