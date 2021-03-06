package im.actor.federation

import akka.actor.UntypedActor
import com.rabbitmq.client.*
import java.io.IOException
import java.util.concurrent.CompletableFuture

abstract class ClusterConnection(val address: List<Address>, val rmqUserName: String, val rmqPassword: String,
                                 val vhost: String, val prefix: String) : UntypedActor() {

    private var connectionFactory: ConnectionFactory? = ConnectionFactory().apply {
        this.username = rmqUserName
        this.password = rmqPassword
        this.virtualHost = vhost
        this.requestedHeartbeat = 5
    }
    private var connection: Connection? = null
    private var channel: Channel? = null
    private var channelIndex: Int = 0

    // Connection Management

    override fun preStart() {
        tryConnection()
    }

    fun tryConnection() {
        // Ignore if connection is already present
        if (this.connection != null) {
            return
        }
        println("Trying to connect to cluster")
        try {
            connection = connectionFactory!!.newConnection(address)
            channel = connection!!.createChannel()
            connection!!.addShutdownListener {
                self().tell(ConnectionDies(), self())
            }
            channel!!.exchangeDeclare("cluster.$prefix", "direct")
            channel!!.queueDeclare("cluster.$prefix.main", true, false, false, null)
            channel!!.queueDeclare("cluster.$prefix.ephemeral", true, false, false, null)
            channel!!.queueDeclare("cluster.$prefix.rpc", true, false, false, null)
            channel!!.queueBind("cluster.$prefix.main", "cluster.$prefix", "main")
            channel!!.queueBind("cluster.$prefix.ephemeral", "cluster.$prefix", "ephemeral")
            channel!!.queueBind("cluster.$prefix.rpc", "cluster.$prefix", "rpc")

            val chId = channelIndex
            channel!!.basicConsumeAck("cluster.$prefix.main") { body, deliveryTag ->
                val future = CompletableFuture<Boolean>()
                future.thenApply {
                    self().tell(AckDelivery(deliveryTag, chId), self())
                }
                self().tell(EventMessage(body, future), self())
            }
            channel!!.basicConsume("cluster.$prefix.ephemeral") {
                self().tell(EphemeralMessage(it), self())
            }
            channel!!.basicConsume("cluster.$prefix.rpc") {
                self().tell(RPCMessage(it), self())
            }
            println("Successfully connected to cluster")
        } catch (_: IOException) {
            freeConnection()
            println("Unable to connect to cluster")
            Thread.sleep(1000)
            self().tell(TryConnection(), self())
        }
    }

    fun connectionDies() {
        freeConnection()
        tryConnection()
    }

    fun freeConnection() {
        if (channel != null) {
            try {
                channel!!.close()
            } catch (_: Exception) {
            }
            channel = null
        }
        if (connection != null) {
            try {
                connection!!.close()
            } catch (_: Exception) {
            }
            connection = null
        }
        channelIndex++
    }

    //
    // Main Loop
    //

    abstract fun onEventMessage(data: ByteArray, future: CompletableFuture<Boolean>)

    abstract fun onEphemeralMessage(data: ByteArray)

    abstract fun onRpcMessage(data: ByteArray)

    fun sendEventMessage(dest: String, data: ByteArray) {
        channel!!.basicPublish("cluster.$dest", "main", AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .build(), data)
    }

    private fun onMessageAck(deliveryTag: Long, chId: Int) {
        if (channelIndex == chId) {
            channel!!.basicAck(deliveryTag, false)
        }
    }

    //
    // Shutdown
    //

    override fun postStop() {
        super.postStop()
        if (connection != null) {
            connection!!.close()
            connection = null
            channel = null
        }
    }

    override fun onReceive(message: Any?) {
        when (message) {
            is TryConnection -> tryConnection()
            is ConnectionDies -> connectionDies()
            is EventMessage -> onEventMessage(message.body, message.future)
            is EphemeralMessage -> onEphemeralMessage(message.body)
            is RPCMessage -> onRpcMessage(message.body)
            is AckDelivery -> onMessageAck(message.deliveryTag, message.chId)

            else -> unhandled(message)
        }
    }
}

private class TryConnection()

private class ConnectionDies()

private class EventMessage(val body: ByteArray, val future: CompletableFuture<Boolean>)

private class EphemeralMessage(val body: ByteArray)

private class RPCMessage(val body: ByteArray)

private class AckDelivery(val deliveryTag: Long, val chId: Int)