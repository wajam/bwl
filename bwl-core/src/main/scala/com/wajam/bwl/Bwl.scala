package com.wajam.bwl

import com.wajam.nrv.service.{Resolver, ActionMethod, Action, Service}
import com.wajam.nrv.data.{MInt, MValue, InMessage}
import com.wajam.nrv.data.MValue._
import com.wajam.nrv.utils.{SynchronizedIdGenerator, TimestampIdGenerator}
import com.wajam.bwl.queue.{QueueDefinition, Queue, QueueFactory}
import com.wajam.spnl.feeder.Feeder
import scala.concurrent.{ExecutionContext, Future}
import com.wajam.nrv.InvalidParameter

class Bwl(name: String = "bwl", definitions: Iterable[QueueDefinition], factory: QueueFactory) extends Service(name) {

  private implicit def msg2request(msg: InMessage) = new ApiRequest(msg)

  private var timestampGenerator = new TimestampIdGenerator with SynchronizedIdGenerator[Long]

  private var queues: Map[(Long, String), Queue] = Map()

  applySupport(resolver = Some(new Resolver(tokenExtractor = Resolver.TOKEN_PARAM("token"))))

  /**
   * Creates a SPNL Feeder for the specified queue. Must NEVER EVER creates concurrent running feeders for the same
   * queue unless the previous feeder has previously been killed. The feeder behavior is undetermined if multiple
   * concurrent running feeders are created.
   */
  def createQueueFeeder(token: Long, name: String): Option[Feeder] = {
    queues.get((token, name)).map(_.feeder)
  }

  /**
   * Enqueue the specified task data and returns the task id if enqueued successfully .
   */
  def enqueue(token: Long, name: String, task: Any, priority: Option[Int] = None)
             (implicit ec: ExecutionContext): Future[Long] = {
    var params = List[(String, MValue)]("token" -> token, "name" -> name) ++ priority.map(p => "priority" -> MInt(p))
    val result = enqueueAction.call(params = params, meta = Map(), data = task)
    result.map(response => response.paramLong("id"))
  }

  /**
   * Acknowledge the specified task by id
   */
  private[bwl] def ack(token: Long, name: String, id: Long)
                      (implicit ec: ExecutionContext): Future[Unit] = {
    val result = ackAction.call(params = List("token" -> token, "name" -> name, "id" -> id), meta = Map(), data = null)
    result.map(_ => Unit)
  }

  private val enqueueAction = registerAction(new Action("/queues/:token/:name/tasks", handleEnqueueTask, ActionMethod.POST))

  private val ackAction = registerAction(new Action("/queues/:token/:name/tasks/:id", handleAckTask, ActionMethod.DELETE))

  private def handleEnqueueTask(message: InMessage) {
    val msgToken = message.paramLong("token")
    val name = message.paramString("name")
    val token = resolveMembers(msgToken, 1).head.token

    queues.get((token, name)) match {
      case Some(queue: Queue) => {
        if (message.timestamp.isEmpty) message.timestamp = Some(timestampGenerator.nextId)

        message.paramOptionalInt("priority") match {
          case Some(priority) => {
            queue.enqueue(message, priority)
            message.reply(Map("id" -> message.timestamp.get.toString))
          }
          case None if queue.priorities.size == 1 => {
            // If no priority is specified and queue has only one priority, default to that priority
            queue.enqueue(message, queue.priorities.head.value)
            message.reply(Map("id" -> message.timestamp.get.toString))
          }
          case None => throw new InvalidParameter(s"Parameter priority must be specified")
        }
      }
      case None => throw new InvalidParameter(s"No queue $name for shard $token")
    }
  }

  private def handleAckTask(message: InMessage) {
    val token = message.paramLong("token")
    val name = message.paramString("name")
    val id = message.paramLong("id")

    queues.get((token, name)) match {
      case Some(queue: Queue) => queue.ack(id)
      case None => throw new InvalidParameter(s"No queue $name for shard $token")
    }
  }

  override def start() {
    super.start()

    // Build queues for each queue definition and local service member pair
    val localMembers = members.filter(m => cluster.isLocalNode(m.node)).toList
    queues = definitions.flatMap(d => localMembers.map(m => (m.token, d.name) -> factory(m.token, d))).toMap
    queues.valuesIterator.foreach(_.start())
  }

  override def stop() {
    super.stop()

    queues.valuesIterator.foreach(_.stop())
    queues = Map()
  }
}
