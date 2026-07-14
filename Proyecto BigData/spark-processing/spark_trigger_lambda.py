import json
import os

import boto3

emr = boto3.client("emr")
CLUSTER_ID = os.environ["EMR_CLUSTER_ID"]
BUCKET = os.environ["RESULTS_BUCKET"]


def _buildStep(name, scriptPath, inputPath, outputPath):
    return {
        "Name": name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", scriptPath, inputPath, outputPath],
        },
    }


def handler(event, context):
    body = json.loads(event.get("body", "{}")) if "body" in event else event
    jobId = body["jobId"]
    inputPath = body["inputPath"]

    outputRDD = f"s3://{BUCKET}/results/{jobId}/rdd"
    outputDF = f"s3://{BUCKET}/results/{jobId}/dataframe"

    steps = [
        _buildStep("RDDPipeline", f"s3://{BUCKET}/scripts/rdd_pipeline.py", inputPath, outputRDD),
        _buildStep("DataFramePipeline", f"s3://{BUCKET}/scripts/dataframe_pipeline.py", inputPath, outputDF),
    ]

    response = emr.add_job_flow_steps(JobFlowId=CLUSTER_ID, Steps=steps)
    return {"statusCode": 202, "body": json.dumps({"jobId": jobId, "stepIds": response["StepIds"]})}
