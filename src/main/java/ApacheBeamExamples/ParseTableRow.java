package ApacheBeamExamples;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

public class ParseTableRow extends DoFn<TableRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        String id = (String) row.get("id");
        String score = (String) row.get("score");
        c.output(String.valueOf(id + "-" + score));
    }
}
