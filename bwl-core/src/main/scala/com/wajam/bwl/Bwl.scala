package com.wajam.bwl

import java.util.concurrent.TimeUnit
import com.wajam.nrv.service.{ ServiceMember, Resolver, Service }
import com.wajam.nrv.data.{ MLong, MValue, MInt }
import com.wajam.nrv.data.MValue._
import com.wajam.bwl.queue._
import scala.concurrent.{ ExecutionContext, Future }
import com.wajam.bwl.queue.QueueFactory
import com.wajam.spnl._
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.commons.{ CurrentTime, Logging }
import scala.util.Random
import com.yammer.metrics.scala.{ Timer, Counter, Instrumented }

class Bwl(serviceName: String, protected val definitions: Iterable[QueueDefinition], protected val queueFactory: QueueFactory,
          spnl: Spnl, taskPersistenceFactory: TaskPersistenceFactory = new NoTaskPersistenceFactory)(implicit random: Random = Random)
    extends Service(serviceName) with Logging {

  val metricsPerQueue = definitions.map { definition =>
    definition.name -> new BwlMetrics(serviceName, definition)
  }.toMap

  protected[bwl] def getMetricsClass: Class[_] = classOf[Bwl]

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

  def queueViews(serviceName: String): Iterable[QueueView] = queues.valuesIterator.map { wrapper =>
    new QueueView {
      def name = wrapper.queue.name

      def priorities = wrapper.queue.priorities

      def stats = wrapper.queue.stats
    }
  }.toIterable

  /**
   * Enqueue the specified task data and returns the task id if enqueued successfully .
   */
  def enqueue(token: Long, name: String, taskData: Any, priority: Option[Int] = None, delay: Option[Long] = None)(implicit ec: ExecutionContext): Future[Long] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._
    import QueueResource._

    val action = queueResource.create(this).get

    val params = List[(String, MValue)](TaskToken -> token, QueueName -> name) ++ priority.map(p => TaskPriority -> MInt(p)) ++ delay.map(t => TaskScheduleTime -> MLong(delayToScheduleTime(t)))
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
    val queue = queueFactory.createQueue(member.token, definition, this)
    val persistence = taskPersistenceFactory.createServiceMemberPersistence(this, member)

    // TODO: allow per queue timeout???
    val taskAction = new TaskAction(s"${serviceName}_${definition.name}_${member.token}", queueCallbackAdapter(definition), responseTimeout)
    val task = new Task(queue.feeder, taskAction, persistence, queue.definition.taskContext, random)

    QueueWrapper(queue, task)
  }

  // Compute the elapsed time after which a callback does not result to a task acknowledgement even if successful.
  // After that duration, it is assumed that SPNL has already timed out and scheduled a retry for the task.
  private def callbackTimeout = math.max(responseTimeout * 0.75, responseTimeout - 500)

  private def queueCallbackAdapter(definition: QueueDefinition)(request: SpnlRequest) {
    import QueueResource._

    // Load metrics for this queue
    val metrics = metricsPerQueue(definition.name)
    import metrics._

    implicit val sameThreadExecutionContext = new ExecutionContext {
      def execute(runnable: Runnable) {
        runnable.run()
      }

      def reportFailure(t: Throwable) {
        log.error("Failure in BWL queue future callback: " + t)
      }
    }

    val data = request.message.getData[TaskData]
    val taskToken = data.token
    val taskId = data.values(TaskId).toString.toLong
    val priority = data.values(TaskPriority).toString.toInt

    definition.maxRetryCount match {
      case Some(maxRetryCount) if data.retryCount > maxRetryCount => {
        // Task max retry count reach. Do not execute callback.
        resultRetryMaxReached += 1
        ack(taskToken, definition.name, taskId, priority)
        val msg = s"Task $taskId ($taskToken:${definition.name}#$priority) not executed. Maximum retry count ($maxRetryCount) reached."
        request.ignore(new Exception(msg))
        warn(msg)
      }
      case _ => executeCallback()
    }

    def executeCallback() {
      val startTime = System.currentTimeMillis()

      def elapsedTime = System.currentTimeMillis() - startTime

      def executeIfCallbackNotExpired(executedTimer: Timer, expiredTimer: Timer)(function: => Any) {
        trace(s"'Task $taskId ($taskToken:${definition.name}#$priority) callback elapsedTime: $elapsedTime")
        if (elapsedTime < callbackTimeout) {
          executedTimer.update(elapsedTime, TimeUnit.MILLISECONDS)
          function
        } else {
          expiredTimer.update(elapsedTime, TimeUnit.MILLISECONDS)
          warn(s"Task $taskId ($taskToken:${definition.name}#$priority) callback took too much time to execute ($elapsedTime ms)")
        }
      }

      val response = definition.callback.execute(data.values("data"))
      response.onSuccess {
        case QueueCallback.Result.Ok => executeIfCallbackNotExpired(resultOkTimer, resultExpiredOkTimer) {
          ack(taskToken, definition.name, taskId, priority)
          request.ok()
        }
        case QueueCallback.Result.Fail(error, ignore) if ignore => executeIfCallbackNotExpired(resultIgnoreTimer, resultExpiredIgnoreTimer) {
          ack(taskToken, definition.name, taskId, priority)
          request.ignore(error)
        }
        case QueueCallback.Result.Fail(error, ignore) => executeIfCallbackNotExpired(resultFailTimer, resultExpiredFailTimer) {
          request.fail(error)
        }
      }
      response.onFailure {
        case e: Exception =>
          exceptionTimer.update(elapsedTime, TimeUnit.MILLISECONDS)
          request.fail(e)
        case t =>
          exceptionTimer.update(elapsedTime, TimeUnit.MILLISECONDS)
          request.fail(new Exception(t))
      }
    }
  }

  private def delayToScheduleTime(delay: Long) = System.currentTimeMillis() + delay

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

  class BwlMetrics(serviceName: String, definition: QueueDefinition) extends Instrumented {
    val metricsClass = getMetricsClass
    val metricsScope = s"$serviceName.${definition.name}"

    def timer(name: String) = new Timer(metrics.metricsRegistry.newTimer(metricsClass, name, metricsScope))
    def counter(name: String) = new Counter(metrics.metricsRegistry.newCounter(metricsClass, name, metricsScope))

    val resultOkTimer = timer("callback-result-ok-time")
    val resultIgnoreTimer = timer("callback-result-ignore-time")
    val resultFailTimer = timer("callback-result-fail-time")
    val resultRetryMaxReached = counter("callback-result-retry-max-reached")
    val resultExpiredOkTimer = timer("callback-result-expired-ok-time")
    val resultExpiredIgnoreTimer = timer("callback-result-expired-ignore-time")
    val resultExpiredFailTimer = timer("callback-result-expired-fail-time")
    val exceptionTimer = timer("callback-exception-time")
  }
}
