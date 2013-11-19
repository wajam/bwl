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
                           maxRetryCount: Option[Int] = None)

sealed trait QueueItem {
  def itemId: Timestamp
  def token: Long
  def priority: Int
  def name: String
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

trait QueueView {

  def name: String

  def priorities: Iterable[Priority]

  def stats: QueueStats
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
  def createQueue(token: Long, definition: QueueDefinition, service: Service): Queue
}

trait QueueCallback {
  def execute(data: Any): Future[QueueCallback.Result]
}

object QueueCallback {

  sealed trait Result

  object Result {

    object Ok extends Result

    case class Fail(error: Exception, ignore: Boolean = false) extends Result
  }
}
