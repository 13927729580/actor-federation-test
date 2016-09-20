package im.actor.federation

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedActor
import com.typesafe.config.ConfigFactory
import im.actor.federation.api.*

fun main(args: Array<String>) {
    val system = ActorSystem.create()

    system.actorOf(Props.create({ return@create TestServer("actor.im", "81port.com".reverseDomain()) }))
    for (i in 0..50) {
        system.actorOf(Props.create({ return@create TestServer("81port-$i.com", "actor.im".reverseDomain()) }))
    }

    system.awaitTermination()
}

class TestServer(val domain: String, val dest: String) : UntypedActor() {

    val config = ConfigFactory.load()!!
    var connection: ActorRef? = null
    var index: Int = 0

    override fun preStart() {
        super.preStart()

        val userName = config.getString("actor.cluster.username")
        val password = config.getString("actor.cluster.password")
        // val domain = "actor.im"// config.getString("actor.cluster.domain")
        val sdomain = domain
        connection = context().actorOf(Props.create({
            return@create ClusterActorConnection(userName, password, sdomain)
        }))

        self().tell(TestEvent(), self())
    }

    override fun onReceive(message: Any?) {
        when (message) {
            is ClusterEvent -> {
                // Log Event
                println("Cluster Event: " + message.event)
                // Mark as received
                message.future.complete(true)


            }
            is TestEvent -> {
                for (i in 1..50) {

                    val event = Event.newBuilder()
                            .setMessage(MessageEvent.newBuilder()
                                    .setSender(User.newBuilder()
                                            .setId("123")
                                            .setRoute(UserRoute.newBuilder()
                                                    .setServer("matrix.org")
                                                    .setKey("user1"))
                                            .setInfo(UserInfo.newBuilder()
                                                    .setName("Hey!")
                                                    .setDomain("matrix.org")))
                                    .setContent(MessageContent.newBuilder()
                                            .setText(TextMessage.newBuilder()
                                                    .setText("Hey!")))
                                    .setRouting(UserRoute.newBuilder()
                                            .setKey("steve@actor.im")
                                            .setServer("actor.im")))
                            .build()

                    connection!!.tell(OutClusterEvent(event, dest), self())
                }
                self().tell(message, self())
            }
        }
    }
}

class TestEvent {

}