package ApacheBeamExamples.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;
import java.util.ArrayList;

public class MsgParser extends DoFn<PubsubMessage, TableRow> {
    public static TupleTag<TableRow> SuccessfulParse = new TupleTag<>();
    @ProcessElement
    public void processElement(ProcessContext c) {
        byte[] msg = c.element().getPayload();
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            News news = objectMapper.readValue(msg, News.class);
            c.output(SuccessfulParse, buildTableRowFor(news));
        } catch (IOException e) {
            DeadLetterHandler.DeadLetterError deadLetterError = new DeadLetterHandler.DeadLetterError(
                e, c.element(), c.timestamp()
            );
            c.output(DeadLetterHandler.DeadLetterTag, deadLetterError);
        }
    }

    public static TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setFields(new ArrayList<TableFieldSchema>() {
            {
                add(new TableFieldSchema().setName("by").setType("STRING").setMode("NULLABLE"));
                add(new TableFieldSchema().setName("score").setType("INTEGER").setMode("NULLABLE"));
                add(new TableFieldSchema().setName("title").setType("STRING").setMode("NULLABLE"));
            }
        });
        return tableSchema;
    }

    private class News {
        String by;
        Integer score;
        String title;
    }

    private TableRow buildTableRowFor(News news) {
        TableRow tableRow = new TableRow();
        tableRow.set("by", news.by);
        tableRow.set("score", news.score);
        tableRow.set("title", news.title);
        return tableRow;
    }
}
