package orchestrator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object ResultAnalyzerActor {
  def apply(responseActor: ActorRef[Stage]): Behavior[Stage] = Behaviors.receiveMessage {
    case AnalyzeResults(jobId, spark, replyTo) =>
      val detail = f"RDD: ${spark.rddSeconds}%.3fs, DataFrame: ${spark.dataframeSeconds}%.3fs, speedup ${spark.speedup}%.2fx"
      responseActor ! Respond(jobId, "COMPLETED", detail, replyTo)
      Behaviors.same
    case _ => Behaviors.same
  }
}
