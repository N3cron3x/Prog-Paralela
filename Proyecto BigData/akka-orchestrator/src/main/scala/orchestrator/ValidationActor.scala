package orchestrator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object ValidationActor {
  def apply(gpuActor: ActorRef[Stage]): Behavior[Stage] = Behaviors.receiveMessage {
    case Validate(req, replyTo) =>
      if (req.data.isEmpty) replyTo ! FinalResponse(req.jobId, "ERROR", "El arreglo de entrada no puede estar vacio")
      else gpuActor ! DispatchGpu(req.jobId, req.data, replyTo)
      Behaviors.same
    case _ => Behaviors.same
  }
}
