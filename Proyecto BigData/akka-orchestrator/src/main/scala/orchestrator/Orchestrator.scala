package orchestrator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import software.amazon.awssdk.services.lambda.LambdaClient
import software.amazon.awssdk.services.s3.S3Client

object Orchestrator {
  def apply(gpuFunction: String, sparkTriggerFunction: String, resultsBucket: String): Behavior[Stage] =
    Behaviors.setup { context =>
      val lambdaClient = LambdaClient.create()
      val s3Client = S3Client.create()

      val responseActor = context.spawn(ResponseActor(), "response-actor")
      val analyzerActor = context.spawn(ResultAnalyzerActor(responseActor), "analyzer-actor")
      val sparkActor = context.spawn(SparkJobActor(lambdaClient, s3Client, sparkTriggerFunction, resultsBucket, analyzerActor), "spark-actor")
      val gpuActor = context.spawn(GpuPreprocessActor(lambdaClient, gpuFunction, sparkActor), "gpu-actor")
      val validationActor = context.spawn(ValidationActor(gpuActor), "validation-actor")

      Behaviors.receiveMessage { message =>
        validationActor ! message
        Behaviors.same
      }
    }
}
