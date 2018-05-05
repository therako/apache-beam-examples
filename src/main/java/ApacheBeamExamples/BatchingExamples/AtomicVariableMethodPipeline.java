package ApacheBeamExamples.BatchingExamples;

import ApacheBeamExamples.Utils.ParseTableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AtomicVariableMethodPipeline {
    public static void main(String args[]) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
            .as(DataflowPipelineOptions.class);
        final Pipeline pipeline = Pipeline.create(pipelineOptions);
        final String query = "SELECT id, score from [bigquery-public-data:hacker_news.stories]";
        Long batchSize = 10L;

        pipeline
            .apply("ReadItems", BigQueryIO.readTableRows().fromQuery(query))
            .apply("ParseTableRow", ParDo.of(new ParseTableRow()))
            .apply("ConcurrentQueueAggregate", ParDo.of(new DoFn<String, Iterable<String>>() {
                private Queue<String> queue = new ConcurrentLinkedQueue<>();

                @ProcessElement
                void processElement(ProcessContext c) {
                    queue.add(c.element());
                    if(queue.size() == batchSize) {
                        c.output(queue);
                        queue.clear();
                    }
                }
            }))
            .apply("BatchPredict", ParDo.of(new BatchPredict()))
            .apply("StoreResults", TextIO.write());

        pipeline.run();
    }
}
