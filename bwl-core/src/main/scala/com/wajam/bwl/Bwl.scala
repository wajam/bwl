package com.wajam.bwl

import com.wajam.nrv.service.{Resolver, Service}
import com.wajam.nrv.data.{MInt, MValue}
import com.wajam.nrv.data.MValue._
import com.wajam.bwl.queue.{QueueService, QueueDefinition, Queue}
import com.wajam.spnl.feeder.Feeder
import scala.concurrent.{ExecutionContext, Future}
import com.wajam.bwl.queue.Queue.QueueFactory

class Bwl(name: String = "bwl", definitions: Iterable[QueueDefinition], factory: QueueFactory)
  extends Service(name) with QueueService {

  private var queues: Map[(Long, String), Queue] = Map()

  applySupport(resolver = Some(new Resolver(tokenExtractor = Resolver.TOKEN_PARAM("token"))))

  val queueResource = new QueueResource((token, name) => queues.get(token, name), token => resolveMembers(token, 1).head)
  queueResource.registerTo(this)

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
  def enqueue(token: Long, name: String, task: Any, priority: Option[Int] = None)(implicit ec: ExecutionContext): Future[Long] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._
    import QueueResource._

    val action = queueResource.create(this).get

    val params = List[(String, MValue)](TaskToken -> token, QueueName -> name) ++ priority.map(p => Priority -> MInt(p))
    val result = action.call(params = params, meta = Map(), data = task)
    result.map(response => response.param[Long](TaskId))
  }

  /**
   * Acknowledge the specified task by id
   */
  private[bwl] def ack(token: Long, name: String, id: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    import QueueResource._

    val action = queueResource.delete(this).get

    val params: List[(String, MValue)] = List(TaskToken -> token, QueueName -> name, TaskId -> id)
    val result = action.call(params = params, meta = Map(), data = null)
    result.map(_ => Unit)
  }

  override def start() {
    super.start()

    // Build queues for each queue definition and local service member pair
    // TODO: Creates/deletes queues when service members goes Up/Down
    val localMembers = members.filter(m => cluster.isLocalNode(m.node)).toList
    queues = definitions.flatMap(d => localMembers.map(m => (m.token, d.name) -> factory(m.token, d, this))).toMap
    queues.valuesIterator.foreach(_.start())
  }

  override def stop() {
    super.stop()

    queues.valuesIterator.foreach(_.stop())
    queues = Map()
  }
}
