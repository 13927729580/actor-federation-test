package im.actor.federation

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedActor
import com.typesafe.config.ConfigFactory

fun main(args: Array<String>) {
    val system = ActorSystem.create()

    system.actorOf(Props.create({ return@create TestServer() }))


    system.awaitTermination()
}

class TestServer : UntypedActor() {

    val config = ConfigFactory.load()!!

    override fun preStart() {
        super.preStart()

        val userName = config.getString("actor.cluster.username")
        val password = config.getString("actor.cluster.password")
        val domain = config.getString("actor.cluster.domain")

        context().actorOf(Props.create({
            return@create ClusterActorConnection(userName, password, domain)
        }))
    }

    override fun onReceive(message: Any?) {

    }
}