package orchestrator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.lambda.LambdaClient
import software.amazon.awssdk.services.lambda.model.InvokeRequest
import spray.json._
import orchestrator.JsonProtocol._

object GpuPreprocessActor {
  private val MAX_RETRIES = 3

  def apply(lambdaClient: LambdaClient, functionName: String, sparkActor: ActorRef[Stage]): Behavior[Stage] =
    Behaviors.setup { context =>

      def invoke(jobId: String, data: List[Float], attempt: Int, replyTo: ActorRef[FinalResponse]): Unit = {
        try {
          val payload = JsObject("data" -> data.toJson).compactPrint
          val request = InvokeRequest.builder()
            .functionName(functionName)
            .payload(SdkBytes.fromUtf8String(payload))
            .build()
          val response = lambdaClient.invoke(request)
          val body = response.payload().asUtf8String().parseJson.asJsObject
          val normalized = body.fields("normalized").convertTo[List[Float]]
          val minVal = body.fields("min").convertTo[Float]
          val maxVal = body.fields("max").convertTo[Float]
          sparkActor ! DispatchSpark(jobId, GpuResult(jobId, normalized, minVal, maxVal), replyTo)
        } catch {
          case ex: Exception =>
            // Reintento acotado ante fallos de invocacion (timeout, throttling, etc.)
            if (attempt < MAX_RETRIES) context.self ! GpuInvokeFailed(jobId, attempt + 1, data, replyTo)
            else replyTo ! FinalResponse(jobId, "ERROR", s"Fallo la etapa GPU tras $MAX_RETRIES intentos: ${ex.getMessage}")
        }
      }

      Behaviors.receiveMessage {
        case DispatchGpu(jobId, data, replyTo) => invoke(jobId, data, 1, replyTo); Behaviors.same
        case GpuInvokeFailed(jobId, attempt, data, replyTo) => invoke(jobId, data, attempt, replyTo); Behaviors.same
        case _ => Behaviors.same
      }
    }
}
