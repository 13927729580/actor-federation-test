package im.actor.federation

import com.rabbitmq.client.Address
import java.util.concurrent.CompletableFuture
import com.github.salomonbrys.kotson.*
import com.google.gson.Gson
import java.io.Serializable

class ClusterActorConnection(rmqUserName: String,
                             rmqPassword: String,
                             val domain: String) :
        ClusterConnection(
                listOf(Address("cluster0.orcarium.com"),
                        Address("cluster1.orcarium.com"),
                        Address("cluster2.orcarium.com"),
                        Address("cluster3.orcarium.com")),
                rmqUserName, rmqPassword, "orca", domain.reverseDomain()) {

    private val gson = Gson()

    override fun onEventMessage(data: ByteArray, future: CompletableFuture<Boolean>) {
        try {
            val event = gson.fromJson<Event>(String(data))
            context.parent().tell(ClusterEvent(event, future), self())
        } catch (e: Exception) {
            e.printStackTrace()
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

data class ClusterEvent(val event: Event, val future: CompletableFuture<Boolean>)

data class Event(
        val eventId: String?,
        val eventType: String?,
        val message: Message?
) : Serializable

data class Message(
        val sender: User,
        val name: String,
        val routingKey: String
) : Serializable

data class User(val name: String,
           val userName: String,
           val routingKey: String,
           val emails: List<String>,
           val phones: List<String>
) : Serializable