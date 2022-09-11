package org.apache.beam.batch;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.help.Concat;
import org.apache.beam.help.PrintFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ConcatPerKey {

  public static void main(String[] args) {

    final List<String> LINES = Arrays.asList(
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ");

    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> input = 
        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        .apply(Filter.by((String word) -> !word.isEmpty()));

    PCollection<KV<String, String>> keyed =
      input.apply(WithKeys.of(new SerializableFunction<String, String>() {
        @Override
        public String apply(String s) {
          return s;
        }
      }));
    
    PCollection<KV<String, String>> concats = keyed.apply(Concat.perKey());

    concats
      .apply(
        MapElements.into(TypeDescriptors.strings())
          .via(
              (KV<String, String> wordC) -> 
                wordC.getKey() + "<>" + wordC.getValue()))
      .apply(ParDo.of(new PrintFn()));

    p.run().waitUntilFinish();
  }
}
