package orchestrator

import spray.json.DefaultJsonProtocol

final case class ProcessRequest(jobId: String, data: List[Float])
final case class GpuResult(jobId: String, normalized: List[Float], min: Float, max: Float)
final case class SparkResult(jobId: String, rddSeconds: Double, dataframeSeconds: Double, speedup: Double)
final case class FinalResponse(jobId: String, status: String, detail: String)

object JsonProtocol extends DefaultJsonProtocol {
  implicit val processRequestFormat = jsonFormat2(ProcessRequest)
  implicit val finalResponseFormat = jsonFormat3(FinalResponse)
}
