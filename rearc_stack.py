from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_lambda_event_sources as lambda_events,
    aws_events as events,
    aws_events_targets as targets,
    Duration
)
from constructs import Construct
import os

class RearcStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        BUCKET_NAME = "rearc-dataquest-quest"

        # Import your existing S3 bucket
        bucket = s3.Bucket.from_bucket_name(
            self, "ExistingBucket", BUCKET_NAME
        )

        # SQS Queue
        queue = sqs.Queue(self, "RearcQueue",
                          visibility_timeout=Duration.seconds(300))

        # Lambda A (sync)
        sync_lambda = _lambda.Function(
            self, "SyncLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda_sync"),
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": BUCKET_NAME,
                "TS_KEY": "pr.data.0.Current",
                "POP_KEY": "us_population.json"
            }
        )

        bucket.grant_read_write(sync_lambda)

        # Daily schedule for Sync Lambda
        rule = events.Rule(
            self, "DailyRule",
            schedule=events.Schedule.cron(minute="0", hour="2")
        )
        rule.add_target(targets.LambdaFunction(sync_lambda))

        # Lambda B (report lambda) — triggered by SQS
        report_lambda = _lambda.Function(
            self, "ReportLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="report.handler",
            code=_lambda.Code.from_asset("lambda_report"),
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": BUCKET_NAME,
                "TS_KEY": "pr.data.0.Current",
                "POP_KEY": "us_population.json"
            }
        )

        bucket.grant_read(report_lambda)

        # Add SQS trigger to report lambda
        report_lambda.add_event_source(
            lambda_events.SqsEventSource(queue)
        )
