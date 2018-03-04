package ApacheBeamExamples;

import ApacheBeamExamples.Utils.CountFn;
import ApacheBeamExamples.Utils.DeadLetterHandler;
import ApacheBeamExamples.Utils.MsgParser;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

public class DeadLetterPatternExample {
    public static void main(String args[]) {
        DeadLetterPatternPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(DeadLetterPatternPipelineOptions.class);
        final Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(pipelineOptions);

        PCollectionTuple parseMsg = pipeline
            .apply("PubSubReader",
                PubsubIO.readMessages().fromSubscription(pipelineOptions.getPubSubSubscriber()))
            .apply("ParseMsg",
            ParDo.of(new MsgParser())
                .withOutputTags(MsgParser.SuccessfulParse, TupleTagList.of(DeadLetterHandler.DeadLetterTag)));

        // Happy path - Write to BQ
        parseMsg
            .get(MsgParser.SuccessfulParse)
            .setCoder(TableRowJsonCoder.of())
            .apply("WriteToBQ",
                BigQueryIO.writeTableRows().to(pipelineOptions.getBqTable())
                    .withSchema(MsgParser.getTableSchema()));

        PCollection<TableRow> deadLetterErrorPCollection = parseMsg
            .get(DeadLetterHandler.DeadLetterTag)
            .setCoder(AvroCoder.of(DeadLetterHandler.DeadLetterError.class))
            .apply("BuildErrorRecord", ParDo.of(new DeadLetterHandler.BuildErrorRecord()));

        // Errors - Write raw data to BQ along with error details
        deadLetterErrorPCollection
            .apply("WriteErrorsToBQ",
                BigQueryIO.writeTableRows().to(pipelineOptions.getBqErrorsTable())
                    .withSchema(DeadLetterHandler.DeadLetterError.getTableSchema()));

        // Errors - Notify via slack on high error rates.
        deadLetterErrorPCollection
            .apply("ErrorsCounterWindow",
                Window.into(FixedWindows.of(Duration.standardSeconds(pipelineOptions.getErrorRateWindow()))))
            .apply("CountErrors", Combine.globally(new CountFn<TableRow>()).withoutDefaults())
            .setCoder(BigEndianLongCoder.of())
            .apply("NotifySlackOnHighErrorRate",
                ParDo.of(new DeadLetterHandler.NotifySlack(
                    pipelineOptions.getMaxErrorRate(),
                    pipelineOptions.getSlackKey(),
                    pipelineOptions.getSlackChannel(),
                    pipelineOptions.getJobName())));

        pipeline.run();
    }

    private interface DeadLetterPatternPipelineOptions extends DataflowPipelineOptions {
        @Description("Slack auth token")
        String getSlackKey();
        void setSlackKey(String slackKey);

        @Description("Slack Channel. fyi, the bot should be part for the channel")
        String getSlackChannel();
        void setSlackChannel(String slackChannel);

        @Description("A Google PubSub subscriber.")
        @Default.String("")
        String getPubSubSubscriber();
        void setPubSubSubscriber(String pubSubSubscriber);


        @Description("Destination BQ full table name")
        @Default.String("")
        String getBqTable();
        void setBqTable(String bqTable);

        @Description("BQ Error logs full table name")
        @Default.String("")
        String getBqErrorsTable();
        void setBqErrorsTable(String bqErrorsTable);

        @Description("BQ Error rate counter window time (in Seconds)")
        @Default.Long(10)
        Long getErrorRateWindow();
        void setErrorRateWindow(Long errorRateWindow);

        @Description("Max allowed error rate in the given window")
        @Default.Long(0)
        Long getMaxErrorRate();
        void setMaxErrorRate(Long maxErrorRate);

    }
}
