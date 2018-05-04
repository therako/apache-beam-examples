package ApacheBeamExamples.BatchingExamples;

import ApacheBeamExamples.Utils.ParseTableRow;
import ApacheBeamExamples.Utils.ToList;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class FixedWindowMethodPipeline {
    public static void main(String args[]) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
            .as(DataflowPipelineOptions.class);
        final Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(pipelineOptions);
        final String query = "SELECT id, score from [bigquery-public-data:hacker_news.stories]";
        Long batchSize = 10L;

        pipeline
            .apply("ReadItems", BigQueryIO.readTableRows().fromQuery(query))
            .apply("ParseTableRow", ParDo.of(new ParseTableRow()))
            .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply("CombineItems", Combine.globally(new ToList<>()))
            .apply("AggregateToBatchOfN", ParDo.of(new AggregateToBatchOfN<>(batchSize)))
            .apply("BatchPredict", ParDo.of(new BatchPredict()))
            .apply("StoreResults", TextIO.write());

        pipeline.run();
    }
}
