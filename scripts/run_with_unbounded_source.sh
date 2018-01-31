#!/bin/bash -e

mvn package && \
    java -cp target/apache-beam-examples-bundled-1.0.1.jar \
        ApacheBeamExamples.BatchOrStreamingPipeline \
        --jobName=BoundedSourcePipeline \
        --isSourceBounded=false \
        --pubSubSubscriber="projects/<gcp_project_id>/subscriptions/<subscriber_name>" \
        --runner=DirectRunner