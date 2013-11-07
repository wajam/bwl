package com.wajam.bwl

import com.wajam.nrv.service.{ ServiceMember, Resolver, Service }
import com.wajam.nrv.data.MValue
import com.wajam.nrv.data.MValue._
import com.wajam.bwl.queue._
import scala.concurrent.{ ExecutionContext, Future }
import com.wajam.bwl.queue.Queue.QueueFactory
import com.wajam.spnl._
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.nrv.data.MInt
import com.wajam.commons.Logging

class Bwl(serviceName: String, val definitions: Iterable[QueueDefinition], protected val createQueue: QueueFactory,
          spnl: Spnl, taskPersistenceFactory: TaskPersistenceFactory = new NoTaskPersistenceFactory)
    extends Service(serviceName) with Logging {

  protected case class QueueWrapper(queue: Queue, task: Task) {
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

  private var internalQueues: Map[(Long, String), QueueWrapper] = Map()

  protected def queues: Map[(Long, String), QueueWrapper] = internalQueues

  applySupport(resolver = Some(new Resolver(tokenExtractor = Resolver.TOKEN_PARAM("token"))))

  protected val queueResource = new QueueResource(
    (token, name) => queues.get(token, name).map(_.queue),
    (name) => definitionFor(name),
    token => resolveMembers(token, 1).head)
  queueResource.registerTo(this)

  private val definitionsMap: Map[String, QueueDefinition] =
    definitions.map(definition => definition.name -> definition).toMap

  protected def definitionFor(queueName: String): QueueDefinition = definitionsMap(queueName)

  /**
   * Enqueue the specified task data and returns the task id if enqueued successfully .
   */
  def enqueue(token: Long, name: String, taskData: Any, priority: Option[Int] = None)(implicit ec: ExecutionContext): Future[Long] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._
    import QueueResource._

    val action = queueResource.create(this).get

    val params = List[(String, MValue)](TaskToken -> token, QueueName -> name) ++ priority.map(p => TaskPriority -> MInt(p))
    val result = action.call(params = params, meta = Map(), data = taskData)
    result.map(response => response.param[Long](TaskId))
  }

  /**
   * Acknowledge the specified task
   */
  private[bwl] def ack(token: Long, name: String, id: Long, priority: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    import QueueResource._

    val action = queueResource.delete(this).get

    val params = List[(String, MValue)](TaskToken -> token, QueueName -> name, TaskId -> id, TaskPriority -> priority)
    val result = action.call(params = params, meta = Map(), data = null)
    result.map(_ => Unit)
  }

  private def createQueueWrapper(member: ServiceMember, definition: QueueDefinition): QueueWrapper = {
    val queue = createQueue(member.token, definition, this)
    val persistence = taskPersistenceFactory.createServiceMemberPersistence(this, member)

    // TODO: allow per queue timeout???
    val taskAction = new TaskAction(definition.name, queueCallbackAdapter(definition), responseTimeout)
    val task = new Task(queue.feeder, taskAction, persistence, queue.definition.taskContext)

    QueueWrapper(queue, task)
  }

  // Compute the elapsed time after which a callback does not result to a task acknowledgement even if successful.
  // After that duration, it is assumed that SPNL has already timed out and scheduled a retry for the task.
  private def callbackTimeout = math.max(responseTimeout * 0.75, responseTimeout - 500)

  private def queueCallbackAdapter(definition: QueueDefinition)(request: SpnlRequest) {
    import QueueTask.Result
    import QueueResource._

    implicit val sameThreadExecutionContext = new ExecutionContext {
      def execute(runnable: Runnable) {
        runnable.run()
      }

      def reportFailure(t: Throwable) {
        log.error("Failure in BWL queue future callback: " + t)
      }
    }

    val startTime = System.currentTimeMillis()
    val data = request.message.getData[TaskData]
    val taskToken = data.token
    val taskId = data.values(TaskId).toString.toLong
    val priority = data.values(TaskPriority).toString.toInt

    def executeIfCallbackNotExpired(function: => Any) {
      val elapsedTime = System.currentTimeMillis() - startTime
      trace(s"'Task ${definition.name}:$priority:$taskId' callback elapsedTime: $elapsedTime")
      if (elapsedTime < callbackTimeout) {
        function
      } else {
        warn(s"Task '${definition.name}:$priority:$taskId' callback took too much time to execute ($elapsedTime ms)")
      }
    }

    val response = definition.callback(data.values("data"))
    response.onSuccess {
      case Result.Ok => {
        executeIfCallbackNotExpired {
          ack(taskToken, definition.name, taskId, priority)
          request.ok()
        }
      }
      case Result.Fail(error, ignore) if ignore => {
        executeIfCallbackNotExpired {
          ack(taskToken, definition.name, taskId, priority)
          request.ignore(error)
        }
      }
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
    internalQueues = definitions.flatMap(d => localMembers.map(m => (m.token, d.name) -> createQueueWrapper(m, d))).toMap
    internalQueues.valuesIterator.foreach(_.start())
  }

  override def stop() {
    super.stop()

    internalQueues.valuesIterator.foreach(_.stop())
    internalQueues = Map()
  }
}
