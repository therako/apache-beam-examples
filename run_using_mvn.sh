#!/usr/bin/env bash

mvn compile exec:java -Dexec.mainClass=ApacheBeamExamples.SampleBatchJobPipeline -Dexec.args=" \
    --tempLocation=gs://dataflow-staging-us-central1-104143033771/temp/ \
    --gcpTempLocation=gs://dataflow-staging-us-central1-104143033771/gcp_temp/ \
    --stagingLocation=gs://carousell-dataflow/staging_area/ \
    --project=woven-bonbon-90705 \
    --jobName=testBQ \
    --serviceAccount=dataflow@woven-bonbon-90705.iam.gserviceaccount.com"