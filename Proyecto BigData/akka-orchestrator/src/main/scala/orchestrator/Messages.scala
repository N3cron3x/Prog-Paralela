package orchestrator

import akka.actor.typed.ActorRef

// Cada etapa del pipeline es un mensaje propio, lo que permite que cada actor
// gestione unicamente su responsabilidad (validacion, GPU, Spark, analisis, respuesta)
sealed trait Stage

final case class Validate(request: ProcessRequest, replyTo: ActorRef[FinalResponse]) extends Stage
final case class DispatchGpu(jobId: String, data: List[Float], replyTo: ActorRef[FinalResponse]) extends Stage
final case class DispatchSpark(jobId: String, gpu: GpuResult, replyTo: ActorRef[FinalResponse]) extends Stage
final case class AnalyzeResults(jobId: String, spark: SparkResult, replyTo: ActorRef[FinalResponse]) extends Stage
final case class Respond(jobId: String, status: String, detail: String, replyTo: ActorRef[FinalResponse]) extends Stage

private[orchestrator] final case class GpuInvokeFailed(jobId: String, attempt: Int, data: List[Float], replyTo: ActorRef[FinalResponse]) extends Stage
private[orchestrator] final case class PollSpark(jobId: String, attempt: Int, replyTo: ActorRef[FinalResponse]) extends Stage
