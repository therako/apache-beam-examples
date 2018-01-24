#!/usr/bin/env bash

mvn assembly:assembly

java -cp target/apache-beam-examples-1.0.0-jar-with-dependencies.jar \
    ApacheBeamExamples.SampleBatchJobPipeline \
    --tempLocation=gs://dataflow-staging-us-central1-104143033771/temp/ \
    --gcpTempLocation=gs://dataflow-staging-us-central1-104143033771/gcp_temp/ \
    --stagingLocation=gs://carousell-dataflow/staging_area/ \
    --project=woven-bonbon-90705 \
    --jobName=testBQ \
    --serviceAccount=dataflow@woven-bonbon-90705.iam.gserviceaccount.com