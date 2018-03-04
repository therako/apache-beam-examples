package ApacheBeamExamples.Utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.ullink.slack.simpleslackapi.SlackAttachment;
import com.ullink.slack.simpleslackapi.SlackChannel;
import com.ullink.slack.simpleslackapi.SlackPreparedMessage;
import com.ullink.slack.simpleslackapi.SlackSession;
import com.ullink.slack.simpleslackapi.impl.SlackSessionFactory;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class DeadLetterHandler {
    public static TupleTag<DeadLetterError> DeadLetterTag = new TupleTag<>();

    @DefaultCoder(AvroCoder.class)
    public static class DeadLetterError {
        Exception exception;
        PubsubMessage msg;
        Instant timestamp;

        DeadLetterError(Exception exception, PubsubMessage msg, Instant timestamp) {
            this.exception = exception;
            this.msg = msg;
            this.timestamp = timestamp;
        }

        TableRow toBqTableRow() {
            TableRow tableRow  = new TableRow();
            tableRow.set("error_class", exception.getClass());
            tableRow.set("error_msg", exception.getMessage());
            tableRow.set("error_stack", StackTraceUtil.getFullStackTrace(exception, '\n'));

            tableRow.set("msg", msg.getPayload());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            tableRow.set("time_received", simpleDateFormat.format(timestamp.toDate()));
            return tableRow;
        }

        public static TableSchema getTableSchema() {
            TableSchema tableSchema = new TableSchema();
            tableSchema.setFields(new ArrayList<TableFieldSchema>() {
                {
                    add(new TableFieldSchema().setName("error_class").setType("STRING").setMode("NULLABLE"));
                    add(new TableFieldSchema().setName("error_msg").setType("STRING").setMode("NULLABLE"));
                    add(new TableFieldSchema().setName("error_stack").setType("STRING").setMode("NULLABLE"));
                    add(new TableFieldSchema().setName("msg").setType("STRING").setMode("NULLABLE"));
                    add(new TableFieldSchema().setName("time_received").setType("TIMESTAMP").setMode("NULLABLE"));
                }
            });
            return tableSchema;
        }
    }

    public static class BuildErrorRecord extends DoFn<DeadLetterError, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            DeadLetterError deadLetterError = c.element();
            c.output(deadLetterError.toBqTableRow());
        }
    }

    public static class NotifySlack extends DoFn<Long, Void> {
        private Long maxErrorRate;
        private String authToken;
        private String slackChannel;
        private String jobName;

        public NotifySlack(Long maxErrorRate, String authToken, String slackChannel, String pipelineName) {
            this.maxErrorRate = maxErrorRate;
            this.authToken = authToken;
            this.slackChannel = slackChannel;
            this.jobName = pipelineName;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                Long currentErrorCount = c.element();
                if (currentErrorCount > maxErrorRate) {
                    SlackSession session = SlackSessionFactory.createWebSocketSlackSession(authToken);
                    session.connect();
                    SlackChannel channel = session.findChannelByName(slackChannel);
                    SlackAttachment attachment = new SlackAttachment();
                    attachment.setColor("danger");
                    attachment.addField("JobName", jobName, true);
                    attachment.addField("MaxErrorRate", String.valueOf(this.maxErrorRate), true);
                    attachment.addField("CurrentRate", String.valueOf(currentErrorCount), true);
                    attachment.setTitle("High Error rate");
                    SlackPreparedMessage slackPreparedMessage = new SlackPreparedMessage.Builder()
                        .addAttachment(attachment)
                        .build();
                    session.sendMessage(channel, slackPreparedMessage);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
