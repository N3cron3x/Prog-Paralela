package orchestrator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.lambda.LambdaClient
import software.amazon.awssdk.services.lambda.model.InvokeRequest
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import spray.json._
import orchestrator.JsonProtocol._

import scala.concurrent.duration._
import scala.util.Try

object SparkJobActor {
  private val MAX_POLL_ATTEMPTS = 10
  private val POLL_INTERVAL = 5.seconds

  def apply(
      lambdaClient: LambdaClient,
      s3Client: S3Client,
      triggerFunction: String,
      resultsBucket: String,
      analyzerActor: ActorRef[Stage]
  ): Behavior[Stage] = Behaviors.withTimers { timers =>

    def readResultJson(key: String): Try[JsObject] = Try {
      val req = GetObjectRequest.builder().bucket(resultsBucket).key(key).build()
      val bytes = s3Client.getObject(req).readAllBytes()
      new String(bytes).parseJson.asJsObject
    }

    def fetchSparkResult(jobId: String): Option[SparkResult] = {
      for {
        rdd <- readResultJson(s"results/$jobId/rdd/part-00000").toOption
        df <- readResultJson(s"results/$jobId/dataframe/part-00000").toOption
      } yield {
        val rddSeconds = rdd.fields("elapsedSeconds").convertTo[Double]
        val dfSeconds = df.fields("elapsedSeconds").convertTo[Double]
        SparkResult(jobId, rddSeconds, dfSeconds, if (dfSeconds > 0) rddSeconds / dfSeconds else 0.0)
      }
    }

    Behaviors.receiveMessage {
      case DispatchSpark(jobId, _, replyTo) =>
        val payload = JsObject(
          "jobId" -> jobId.toJson,
          "inputPath" -> s"s3://$resultsBucket/input/$jobId.txt".toJson
        ).compactPrint
        val request = InvokeRequest.builder()
          .functionName(triggerFunction)
          .payload(SdkBytes.fromUtf8String(payload))
          .build()
        lambdaClient.invoke(request)
        timers.startSingleTimer(PollSpark(jobId, 1, replyTo), POLL_INTERVAL)
        Behaviors.same

      case PollSpark(jobId, attempt, replyTo) =>
        fetchSparkResult(jobId) match {
          case Some(result) => analyzerActor ! AnalyzeResults(jobId, result, replyTo)
          case None if attempt < MAX_POLL_ATTEMPTS =>
            timers.startSingleTimer(PollSpark(jobId, attempt + 1, replyTo), POLL_INTERVAL)
          case None =>
            replyTo ! FinalResponse(jobId, "ERROR", "El job de Spark no finalizo dentro del tiempo esperado")
        }
        Behaviors.same

      case _ => Behaviors.same
    }
  }
}
