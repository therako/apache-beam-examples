#!/bin/bash -e

mvn compile exec:java -Dexec.mainClass=ApacheBeamExamples.SampleBatchJobPipeline -Dexec.args=" \
    --tempLocation=gs://<tmp_path>/ \
    --stagingLocation=gs://<staging_path>/ \
    --project=<project_name> \
    --jobName=simpleBatchJob"