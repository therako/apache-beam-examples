#!/bin/bash -e

mvn package && \
    java -cp target/apache-beam-examples-bundled-1.0.1.jar \
        ApacheBeamExamples.BatchOrStreamingPipeline \
        --jobName=BoundedSourcePipeline \
        --isSourceBounded=true \
        --bqQuery="SELECT id, score from [bigquery-public-data:hacker_news.stories]" \
        --runner=DirectRunner \
        --tempLocation=gs://<gcs_tmp_path>/