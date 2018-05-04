package ApacheBeamExamples.BatchingExamples;

import org.apache.beam.sdk.transforms.DoFn;

class BatchPredict extends DoFn<Iterable<String>, String> {

    @ProcessElement
    void processElement(ProcessContext c) {
        // Do Predictions here
        c.output("Results");
    }
}
