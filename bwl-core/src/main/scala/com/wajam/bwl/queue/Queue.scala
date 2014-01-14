package com.wajam.bwl.queue

import com.wajam.spnl.feeder.Feeder
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.utils.{ PeekIteratorOrdering, WeightedItemsSelector }
import com.wajam.nrv.service.Service
import com.wajam.spnl.TaskContext
import scala.concurrent.Future
import com.wajam.spnl.feeder.Feeder.FeederData
import language.implicitConversions
import scala.util.Random

case class Priority(value: Int, weight: Int)

class PrioritySelector(priorities: Iterable[Priority])(implicit random: Random = Random)
  extends WeightedItemsSelector(priorities.map(p => (p.weight.toDouble, p.value)))

case class QueueDefinition(name: String, callback: QueueCallback, taskContext: TaskContext = new TaskContext,
                           priorities: Iterable[Priority] = Seq(Priority(1, weight = 1)),
                           maxRetryCount: Option[Int] = None,
                           callbackTimeout: Option[Long] = None)

sealed trait QueueItem {
  def itemId: Timestamp
  def token: Long
  def priority: Int
  def name: String
  def createTime: Long = itemId.timeMs
}

object QueueItem {

  case class Task(name: String, token: Long, priority: Int, taskId: Timestamp, data: Any, scheduleTime: Option[Long] = None) extends QueueItem {
    def itemId = taskId

    def toFeederData: FeederData = {
      Map("token" -> token, "id" -> taskId.value, "priority" -> priority, "data" -> data)
    }

    def toAck(ackId: Timestamp) = Ack(name, token, priority, ackId, taskId)
  }

  case class Ack(name: String, token: Long, priority: Int, ackId: Timestamp, taskId: Timestamp) extends QueueItem {
    def itemId = ackId
  }

  implicit val Ordering = new Ordering[QueueItem] {
    def compare(x: QueueItem, y: QueueItem) = idFor(x).compareTo(idFor(y))

    private def idFor(item: QueueItem): Timestamp = item match {
      case taskItem: QueueItem.Task => taskItem.taskId
      case ackItem: QueueItem.Ack => ackItem.ackId
    }
  }

  val IteratorOrdering = new PeekIteratorOrdering[QueueItem]
}

trait QueueStats {
  def totalTasks: Int

  def pendingTasks: Iterable[QueueItem.Task]

  def delayedTasks: Iterable[QueueItem.Task]
}

trait QueueRates {

  def currentRate: Double

  def normalRate: Double

  def throttleRate : Double

  def concurrency: Int
}

trait QueueView {

  def token: Long

  def name: String

  def priorities: Iterable[Priority]

  def stats: QueueStats

  def rates: QueueRates
}

trait Queue {

  def token: Long

  def definition: QueueDefinition

  def name: String = definition.name

  def priorities: Iterable[Priority] = definition.priorities

  def enqueue(taskItem: QueueItem.Task): QueueItem.Task

  def ack(ackItem: QueueItem.Ack): QueueItem.Ack

  def feeder: Feeder

  def stats: QueueStats

  def start()

  def stop()
}

trait QueueFactory {
  def createQueue(token: Long, definition: QueueDefinition, service: Service, instrumented: Boolean = true): Queue
}

trait QueueCallback {
  def execute(data: Any): Future[QueueCallback.Result]
}

object QueueCallback {

  sealed trait Result

  sealed trait ResultError extends Result {
    def error: Exception
  }

  object Result {

    object Ok extends Result

    case class Fail(error: Exception, ignore: Boolean = false) extends ResultError

    case class TryLater(error: Exception, delay: Long) extends ResultError {
      require(delay > 0, "TryLater delay must be greater than 0")
    }
  }

  class ResultException(message: String, val result: QueueCallback.ResultError)
      extends Exception(resultMessage(message, result), result.error) {
  }

  private def resultMessage(message: String, result: QueueCallback.ResultError): String = {
    result match {
      case f: QueueCallback.Result.Fail => s"Fail(ignore=${f.ignore}}): $message"
      case tl: QueueCallback.Result.TryLater => s"TryLater(delay=${tl.delay}}): $message"
    }
  }
}
