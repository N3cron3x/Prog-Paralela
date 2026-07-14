package orchestrator

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.lambda.runtime.events.{APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent}
import spray.json._
import orchestrator.JsonProtocol._

import java.util.{HashMap => JHashMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

// Punto de entrada expuesto via API Gateway como integracion proxy con Lambda
class LambdaHandler extends RequestHandler[APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent] {

  private val gpuFunction = sys.env.getOrElse("GPU_FUNCTION_NAME", "gpu-preprocess")
  private val sparkTriggerFunction = sys.env.getOrElse("SPARK_TRIGGER_FUNCTION_NAME", "spark-trigger")
  private val resultsBucket = sys.env.getOrElse("RESULTS_BUCKET", "bigdata-project-results")

  private lazy val system: ActorSystem[Stage] =
    ActorSystem(Orchestrator(gpuFunction, sparkTriggerFunction, resultsBucket), "orchestrator-system")

  private implicit val timeout: Timeout = Timeout(90.seconds)
  private implicit lazy val scheduler = system.scheduler
  private implicit lazy val ec = system.executionContext

  override def handleRequest(input: APIGatewayProxyRequestEvent, context: Context): APIGatewayProxyResponseEvent = {
    val response = new APIGatewayProxyResponseEvent()
    val headers = new JHashMap[String, String]()
    headers.put("Content-Type", "application/json")
    response.setHeaders(headers)

    val attempt: Try[FinalResponse] = Try {
      val body = input.getBody.parseJson.asJsObject
      val jobId = body.fields("jobId").convertTo[String]
      val data = body.fields("data").convertTo[List[Float]]

      val futureResponse: Future[FinalResponse] =
        system.ask[FinalResponse](replyTo => Validate(ProcessRequest(jobId, data), replyTo))

      Await.result(futureResponse, timeout.duration)
    }

    attempt match {
      case Success(finalResponse) =>
        response.setStatusCode(if (finalResponse.status == "COMPLETED") 200 else 500)
        response.setBody(finalResponse.toJson.compactPrint)
      case Failure(ex) =>
        response.setStatusCode(400)
        response.setBody(JsObject("error" -> ex.getMessage.toJson).compactPrint)
    }

    response
  }
}
