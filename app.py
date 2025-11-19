#!/usr/bin/env python3
import aws_cdk as cdk
from rearc_stack import RearcStack

app = cdk.App()
RearcStack(app, "RearcDataPipelineStack")
app.synth()
