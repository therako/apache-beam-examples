package ApacheBeamExamples.utils;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface BatchOrStreamingPipelineOptions extends DataflowPipelineOptions {
    @Description("Defines if the pipeline should use bounded (batch) or unbounded (streaming) source")
    @Default.Boolean(false)
    boolean getIsSourceBounded();
    void setIsSourceBounded(boolean isSourceBounded);

    @Description("If the source is bounded, requires a BQ query here.")
    @Default.String("")
    String getBqQuery();
    void setBqQuery(String bqQuery);


    @Description("If the source is unbounded, requires a PubSub subscriber here.")
    @Default.String("")
    String getPubSubSubscriber();
    void setPubSubSubscriber(String pubSubSubscriber);
}
