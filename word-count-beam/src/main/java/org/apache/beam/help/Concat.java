package org.apache.beam.help;

import java.util.Iterator;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
* {@link PTransform PTransforms} to concats the elements in a {@link PCollection}.
*
* <p>{@link Concat#perElement()} can be used to concat the number of occurrences of each distinct
* element in the PCollection, {@link Concat#perKey()} can be used to concat the number of values per
* key, and {@link Concat#globally()} can be used to concat the total number of elements in a
* PCollection.
*
* <p>{@link #combineFn} can also be used manually, in combination with state and with the {@link
* Combine} transform.
*/
public class Concat {
 private Concat() {
   // do not instantiate
 }

 /** Returns a {@link CombineFn} that concats the number of its inputs. */
 public static <T> CombineFn<T, ?, String> combineFn() {
   return new ConcatFn<>();
 }

 /**
  * Returns a {@link PTransform} that counts the number of elements in its input {@link
  * PCollection}.
  *
  * <p>Note: if the input collection uses a windowing strategy other than {@link GlobalWindows},
  * use {@code Combine.globally(Count.<T>combineFn()).withoutDefaults()} instead.
  */
 public static <T> PTransform<PCollection<T>, PCollection<String>> globally() {
   return Combine.globally(new ConcatFn<T>());
 }

 /**
  * Returns a {@link PTransform} that counts the number of elements associated with each key of its
  * input {@link PCollection}.
  */
 public static <K, V> PTransform<PCollection<KV<K, V>>, PCollection<KV<K, String>>> perKey() {
   return Combine.perKey(new ConcatFn<V>());
 }

 /** A {@link CombineFn} that concatenates elements. */
 private static class ConcatFn<T> extends CombineFn<T, String[], String> {
   // Note that the String[] accumulator always has size 1, used as
   // a box for a mutable String.

   @Override
   public String[] createAccumulator() {
     return new String[] {""};
   }

   @Override
   public String[] addInput(String[] accumulator, T input) {
     String inputStr = input != null ? input.toString() : "";
     accumulator[0] += " " + inputStr;
     return accumulator;
   }

   @Override
   public String[] mergeAccumulators(Iterable<String[]> accumulators) {
     Iterator<String[]> iter = accumulators.iterator();
     if (!iter.hasNext()) {
       return createAccumulator();
     }
     String[] running = iter.next();
     while (iter.hasNext()) {
       running[0] += iter.next()[0];
     }
     return running;
   }

   @Override
   public String extractOutput(String[] accumulator) {
     return accumulator[0];
   }

   @Override
   public boolean equals(@Nullable Object other) {
     return other != null && getClass().equals(other.getClass());
   }

   @Override
   public int hashCode() {
     return getClass().hashCode();
   }

   @Override
   public String getIncompatibleGlobalWindowErrorMessage() {
     return "If the input collection uses a windowing strategy other than GlobalWindows, "
         + "use Combine.globally(Concat.<T>combineFn()).withoutDefaults() instead.";
   }
 }
}
