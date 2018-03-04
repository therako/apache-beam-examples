package ApacheBeamExamples.Utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStrings extends DoFn<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(LogStrings.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String data = c.element();
        logger.info(data);
        c.output(data);
    }
}