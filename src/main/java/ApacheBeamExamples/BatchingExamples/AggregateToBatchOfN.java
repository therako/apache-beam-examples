package ApacheBeamExamples.BatchingExamples;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


class AggregateToBatchOfN<T> extends DoFn<Iterable<T>, Iterable<T>> {
    private Long batchSize;

    AggregateToBatchOfN(Long batchSize) {
        this.batchSize = batchSize;
    }

    @ProcessElement
    void processElement(ProcessContext c) {
        Iterator<T> element = c.element().iterator();
        List<T> output = new ArrayList<>();
        int i = 0;
        while(element.hasNext()) {
            output.add(element.next());
            i++;
            if(i == batchSize) {
                c.output(output);
                output = new ArrayList<>();
                i = 0;
            }
        }
        if(output.size() > 0){
            c.output(output);
        }
    }
}
