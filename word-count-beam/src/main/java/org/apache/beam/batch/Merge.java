package org.apache.beam.batch;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.help.PrintFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Merge {

  public static void main(String[] args) {

    final List<String> LINES = Arrays.asList(
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ");

    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> oneToFour = 
        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

    PCollection<String> fiveToEight = 
        p.apply(Create.of("And by opposing end them. To die—to sleep,", 
            "No more; and by a sleep to say we end",
            "The heart-ache and the thousand natural shocks",
            "That flesh is heir to: 'tis a consummation"
            )).setCoder(StringUtf8Coder.of());

    PCollectionList<String> oneToEightList = 
        PCollectionList.of(oneToFour).and(fiveToEight);
    PCollection<String> oneToEight = 
        oneToEightList.apply(Flatten.<String>pCollections());

    oneToEight.apply(ParDo.of(new PrintFn()));
    
    oneToEight.apply(MapElements.into(TypeDescriptors.strings())
             .via(str -> {
               System.out.println(str);
               return str;
             }));

    p.run().waitUntilFinish();
  }
}
