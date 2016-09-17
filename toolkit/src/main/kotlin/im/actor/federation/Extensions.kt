package im.actor.federation

import akka.japi.Procedure
import akka.persistence.UntypedPersistentActor
import com.rabbitmq.client.*
import java.util.concurrent.CompletableFuture

fun <A : Any> UntypedPersistentActor.persistKt(e: A, f: (() -> Unit)? = null) {
    persist(e, Procedure { if (f != null) f() })
}

fun Channel.basicConsumeAck(queue: String, f: (content: ByteArray, complete: CompletableFuture<Boolean>) -> Unit) {
    basicConsume(queue, false, object : Consumer {
        override fun handleConsumeOk(consumerTag: String?) {

        }

        override fun handleCancel(consumerTag: String?) {

        }

        override fun handleCancelOk(consumerTag: String?) {

        }

        override fun handleRecoverOk(consumerTag: String?) {

        }

        override fun handleShutdownSignal(consumerTag: String?, sig: ShutdownSignalException?) {

        }

        override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
            val future = CompletableFuture<Boolean>()
            future.thenApply {
                basicAck(envelope!!.deliveryTag, false)
            }
            f(body!!, future)
        }
    })
}

fun Channel.basicConsume(queue: String, f: (content: ByteArray) -> Unit) {
    basicConsume(queue, object : Consumer {
        override fun handleConsumeOk(consumerTag: String?) {
        }

        override fun handleCancel(consumerTag: String?) {
        }

        override fun handleCancelOk(consumerTag: String?) {
        }

        override fun handleRecoverOk(consumerTag: String?) {
        }

        override fun handleShutdownSignal(consumerTag: String?, sig: ShutdownSignalException?) {
        }

        override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
            f(body!!)
        }
    })
}