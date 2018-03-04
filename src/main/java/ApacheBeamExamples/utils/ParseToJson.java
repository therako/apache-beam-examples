package ApacheBeamExamples.Utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

public class ParseToJson extends DoFn<TableRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        c.output(row.toString());
    }
}
