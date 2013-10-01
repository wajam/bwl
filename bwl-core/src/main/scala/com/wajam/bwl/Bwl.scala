package com.wajam.bwl

import com.wajam.nrv.service.{ServiceMember, Resolver, Service}
import com.wajam.nrv.data.MValue
import com.wajam.nrv.data.MValue._
import com.wajam.bwl.queue._
import com.wajam.spnl.feeder.Feeder
import scala.concurrent.{ExecutionContext, Future}
import com.wajam.bwl.queue.Queue.QueueFactory
import com.wajam.spnl._
import scala.Some
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.nrv.data.MInt

class Bwl(name: String = "bwl", definitions: Iterable[QueueDefinition], createQueue: QueueFactory,
          spnl: Spnl, taskPersistenceFactory: TaskPersistenceFactory = new NoTaskPersistenceFactory)
  extends Service(name) with QueueService {

  // TODO: find a better name
  private case class BwlQueue(queue: Queue, task: Task) {
    def start() {
      registerAction(task.action.action)
      queue.start()
      spnl.run(task)
    }

    def stop() {
      // TODO: Unregister task action from service or ensure only one is created per queue definition
      spnl.stop(task)
      queue.stop()
    }
  }

  private var queues: Map[(Long, String), BwlQueue] = Map()

  applySupport(resolver = Some(new Resolver(tokenExtractor = Resolver.TOKEN_PARAM("token"))))

  val queueResource = new QueueResource(
    (token, name) => queues.get(token, name).map(_.queue),
    token => resolveMembers(token, 1).head)
  queueResource.registerTo(this)

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

  private def createBwlQueue(member: ServiceMember, definition: QueueDefinition): BwlQueue = {
    val queue = createQueue(member.token, definition, this)
    val persistence = taskPersistenceFactory.createServiceMemberPersistence(this, member)

    // TODO: allow per queue timeout???
    val taskAction = new TaskAction(definition.name, queueCallbackAction(definition), responseTimeout)
    val task = new Task(queue.feeder, taskAction, persistence, queue.definition.taskContext)

    BwlQueue(queue, task)
  }

  private def queueCallbackAction(definition: QueueDefinition)(request: SpnlRequest) {
    import QueueTask.Result

    implicit val sameThreadExecutionContext = new ExecutionContext {
      def execute(runnable: Runnable) {
        runnable.run()
      }

      def reportFailure(t: Throwable) {
        // TODO: log this instead!
        t match {
          case e: Exception => request.fail(e)
          case _ => request.fail(new Exception(t))
        }
      }
    }

    val response = definition.callback(request.message.getData[QueueTask.Data])
    response.onSuccess {
      case Result.Ok => request.ok()
      case Result.Fail(error, ignore) if ignore => request.ignore(error)
      case Result.Fail(error, ignore) => request.fail(error)
    }
    response.onFailure {
      case e: Exception => request.fail(e)
      case t => request.fail(new Exception(t))
    }
  }

  override def start() {
    super.start()

    // Build queues for each queue definition and local service member pair
    // TODO: Creates/deletes queues when service members goes Up/Down
    val localMembers = members.filter(m => cluster.isLocalNode(m.node)).toList
    queues = definitions.flatMap(d => localMembers.map(m => (m.token, d.name) -> createBwlQueue(m, d))).toMap
    queues.valuesIterator.foreach(_.start())
  }

  override def stop() {
    super.stop()

    queues.valuesIterator.foreach(_.stop())
    queues = Map()
  }
}
