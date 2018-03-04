package ApacheBeamExamples.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;

public class MsgParser extends DoFn<PubsubMessage, TableRow> {
    public static TupleTag<TableRow> SuccessfulParse = new TupleTag<>();
    @ProcessElement
    void processElement(ProcessContext c) {
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
