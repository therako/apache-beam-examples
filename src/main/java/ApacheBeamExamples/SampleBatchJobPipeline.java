package ApacheBeamExamples;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SampleBatchJobPipeline {
    public static void main(String args[]) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        final Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(pipelineOptions);
        final String query = "SELECT id, score from [bigquery-public-data:hacker_news.stories]";
        
        setupCommonPipelineOptions(pipelineOptions);

        PCollection<TableRow> tableRowPCollection = pipeline.apply(BigQueryIO.readTableRows().fromQuery(query));
        tableRowPCollection.apply("ParseTableRow", ParDo.of(new ParseTableRow()));
        pipeline.run();
    }

    private static void setupCommonPipelineOptions(DataflowPipelineOptions pipelineOptions) {
        pipelineOptions.setRunner(DataflowRunner.class);
    }

}
