#!/bin/bash -e

mvn package && \
    java -cp target/apache-beam-examples-bundled-1.0.0.jar \
        ApacheBeamExamples.SampleBatchJobPipeline \
        --tempLocation=gs://<tmp_path>/ \
        --stagingLocation=gs://<staging_path>/ \
        --project=<project_name> \
        --jobName=simpleBatchJob