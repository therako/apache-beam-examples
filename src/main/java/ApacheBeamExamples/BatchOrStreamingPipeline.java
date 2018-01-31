package ApacheBeamExamples;


import ApacheBeamExamples.utils.BatchOrStreamingPipelineOptions;
import ApacheBeamExamples.utils.LogStrings;
import ApacheBeamExamples.utils.ParseToJson;
import org.apache.beam.runners.direct.repackaged.runners.core.construction.UnconsumedReads;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.security.InvalidParameterException;

/**
 * BatchOrStreamingPipeline - An example pipeline for switching to Bounded/UnBounded sources based on args
 **/
public class BatchOrStreamingPipeline {
    public static void main(String args[]) {
        BatchOrStreamingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(BatchOrStreamingPipelineOptions.class);
        final Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(pipelineOptions);

        PCollection<String> inputData;
        if(pipelineOptions.getIsSourceBounded()) {
            inputData = readFromBq(pipeline, pipelineOptions);
        } else {
            inputData = readFromPubSub(pipeline, pipelineOptions);
        }
        inputData.apply("LogStrings", ParDo.of(new LogStrings()));
        UnconsumedReads.ensureAllReadsConsumed(pipeline);
        pipeline.run();
    }

    private static PCollection<String> readFromPubSub(Pipeline pipeline,
                                                      BatchOrStreamingPipelineOptions pipelineOptions) {
        if (pipelineOptions.getPubSubSubscriber().isEmpty()) {
            throw new InvalidParameterException("PubSub subscriber not passed in options");
        }
        return pipeline.apply("ReadJsonFromPubSub",
                PubsubIO.readStrings().fromSubscription(pipelineOptions.getPubSubSubscriber()))
                .setCoder(StringUtf8Coder.of());
    }

    private static PCollection<String> readFromBq(Pipeline pipeline, BatchOrStreamingPipelineOptions pipelineOptions) {
        if (pipelineOptions.getBqQuery().isEmpty()) {
            throw new InvalidParameterException("BQ query not passed in options");
        }
        return pipeline
                .apply("ReadTableRowFromBQ", BigQueryIO.readTableRows().fromQuery(pipelineOptions.getBqQuery()))
                .apply("ParseToJson", ParDo.of(new ParseToJson()));
    }
}
