package ApacheBeamExamples.Utils;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine;

@DefaultCoder(AvroCoder.class)
public class CountFn<T> extends Combine.CombineFn<T, CountFn.Accum, Long> {
    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accumulator, T input) {
        accumulator.count++;
        return accumulator;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {
        Accum merged = createAccumulator();
        for (Accum accum : accumulators) {
            merged.count += accum.count;
        }
        return merged;
    }

    @Override
    public Long extractOutput(Accum accumulator) {
        return accumulator.count;
    }

    @DefaultCoder(AvroCoder.class)
    static class Accum {
        Long count = 0L;
    }

}
