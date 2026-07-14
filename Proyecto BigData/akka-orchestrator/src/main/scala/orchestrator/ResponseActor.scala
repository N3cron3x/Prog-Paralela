package orchestrator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object ResponseActor {
  def apply(): Behavior[Stage] = Behaviors.receiveMessage {
    case Respond(jobId, status, detail, replyTo) =>
      replyTo ! FinalResponse(jobId, status, detail)
      Behaviors.same
    case _ => Behaviors.same
  }
}
