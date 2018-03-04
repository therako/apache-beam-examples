#!/bin/bash -e

mvn package && \
    java -cp target/apache-beam-examples-bundled-1.0.1.jar \
        ApacheBeamExamples.DeadLetterPatternExample \
            --jobName=DeadLetterPatternExample \
            --runner=DataflowRunner \
            --pubSubSubscriber="projects/<gcp_project_id>/subscriptions/<subscriber_name>" \
            --project=<gcp_project_id> \
            --stagingLocation=gs://<tmp_gcs_bucket>/ \
            --slackKey=<Slack bot Auth token> \
            --slackChannel=<Slack channel> \
            --bqTable=<destination table> \
            --bqErrorsTable=<error log table> \
            --errorRateWindow=10 \
            --maxErrorRate=1
