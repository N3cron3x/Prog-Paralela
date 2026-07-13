package com.university.actorlambda;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import com.university.actorlambda.actors.SupervisorActor;

/**
 * Punto unico de arranque del ActorSystem.
 *
 * AWS Lambda reutiliza el mismo proceso (y por lo tanto la misma JVM)
 * entre invocaciones consecutivas mientras el contenedor este caliente.
 * Por eso el ActorSystem se guarda en un campo estatico: se crea una
 * sola vez por contenedor y todas las invocaciones posteriores lo
 * reutilizan, en vez de pagar el costo de arrancarlo en cada peticion.
 */
public final class Boot {

    private static final ActorSystem<SupervisorActor.Command> SISTEMA =
            ActorSystem.create(SupervisorActor.create(), "actor-lambda-system");

    private Boot() {
    }

    public static ActorSystem<SupervisorActor.Command> sistema() {
        return SISTEMA;
    }

    public static ActorRef<SupervisorActor.Command> supervisor() {
        return SISTEMA;
    }
}
