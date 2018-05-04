package ApacheBeamExamples.Utils;

import org.apache.beam.sdk.transforms.Combine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ToList<T> extends Combine.CombineFn<T, List<T>, Iterable<T>> {
    @Override
    public List<T> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
        accumulator.add(input);
        return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
        Iterator<List<T>> iter = accumulators.iterator();
        if (!iter.hasNext()) {
            return createAccumulator();
        }
        List<T> res = iter.next();
        while (iter.hasNext()) {
            res.addAll(iter.next());
        }
        return res;
    }

    @Override
    public Iterable<T> extractOutput(List<T> accumulator) {
        return accumulator;
    }
}
