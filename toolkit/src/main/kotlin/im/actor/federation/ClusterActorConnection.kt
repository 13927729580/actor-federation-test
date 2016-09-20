package im.actor.federation

import com.rabbitmq.client.Address
import java.util.concurrent.CompletableFuture
import im.actor.federation.api.Event

class ClusterActorConnection(rmqUserName: String,
                             rmqPassword: String,
                             val domain: String) :
        ClusterConnection(
                listOf(Address("cluster0.orcarium.com"),
                        Address("cluster1.orcarium.com"),
                        Address("cluster2.orcarium.com"),
                        Address("cluster3.orcarium.com")),
                rmqUserName, rmqPassword, "orca", domain.reverseDomain()) {


    override fun onEventMessage(data: ByteArray, future: CompletableFuture<Boolean>) {
        try {
            val event = Event.parseFrom(data)
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

    fun sendEventMessage(dest: String, event: Event) {
        sendEventMessage(dest, event.toByteArray())
    }

    override fun onReceive(message: Any?) {
        when (message) {
            is OutClusterEvent -> sendEventMessage(message.dest, message.event)
            else -> super.onReceive(message)
        }
    }
}

data class OutClusterEvent(val event: Event, val dest: String)

data class ClusterEvent(val event: Event, val future: CompletableFuture<Boolean>)