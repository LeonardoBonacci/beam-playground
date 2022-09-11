package org.apache.beam.help;

import org.apache.beam.sdk.transforms.DoFn;

public class PrintFn extends DoFn<String, String> {

  String prefix;
  
  public PrintFn() {
    this.prefix = "";
  }
  
  public PrintFn(String pre) {
    this.prefix = pre;
  }
  
  @ProcessElement
  public void processElement(@Element String in, OutputReceiver<String> out) {
    System.out.println("-------------------------------");
    System.out.println(prefix + " " + in);
    out.output(in);
  }
}

