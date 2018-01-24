package ApacheBeamExamples;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

public class ParseTableRow extends DoFn<TableRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        int id = (int) row.get("id");
        int score = (int) row.get("score");
        c.output(String.valueOf(id + "-" + score));
    }
}
