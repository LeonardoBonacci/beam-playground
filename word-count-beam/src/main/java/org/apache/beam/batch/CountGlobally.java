package org.apache.beam.batch;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.help.PrintFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CountGlobally {

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

    PCollection<Long> nrWords = input.apply(Count.globally());

    nrWords
      .apply(ToString.elements())
      .apply(ParDo.of(new PrintFn("total count: ")));

    p.run().waitUntilFinish();
  }
}
