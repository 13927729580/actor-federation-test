package im.actor.federation

import com.rabbitmq.client.Address
import java.util.concurrent.CompletableFuture
import com.github.salomonbrys.kotson.*
import com.google.gson.Gson
import java.io.Serializable

class ClusterActorConnection(rmqUserName: String,
                             rmqPassword: String,
                             prefix: String) :
        ClusterConnection(
                listOf(Address("cluster0.orcarium.com"),
                        Address("cluster1.orcarium.com"),
                        Address("cluster2.orcarium.com"),
                        Address("cluster3.orcarium.com")),
                rmqUserName, rmqPassword, "orca", prefix) {

    private val gson = Gson()

    override fun onEventMessage(data: ByteArray, future: CompletableFuture<Boolean>) {
        try {
            val event = gson.fromJson<Event>(String(data))
            context.parent().tell(ClusterEvent(event, future), self())
        } catch (_: Exception) {
            future.complete(false)
        }
    }

    override fun onEphemeralMessage(data: ByteArray) {
        // Not supported yet
    }

    override fun onRpcMessage(data: ByteArray) {
        // Not supported yet
    }
}

class ClusterEvent(val event: Event, val future: CompletableFuture<Boolean>)

class Event(
        val eventId: String,
        val eventType: String?
) : Serializable

class Message(
        val sender: User,
        val name: String,
        val routingKey: String
) : Serializable

class User(val name: String,
           val userName: String,
           val routingKey: String,
           val emails: List<String>,
           val phones: List<String>
) : Serializable