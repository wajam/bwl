package com.wajam.bwl.queue

import com.wajam.spnl.feeder.Feeder
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.utils.WeightedItemsSelector
import com.wajam.nrv.service.Service
import com.wajam.spnl.TaskContext
import scala.concurrent.Future
import com.wajam.spnl.feeder.Feeder.FeederData

// TODO: keep this???
trait QueueService {
  this: Service =>
}

case class Priority(value: Int, weight: Int)

class PrioritySelector(priorities: Iterable[Priority])
  extends WeightedItemsSelector(priorities.map(p => (p.weight.toDouble, p.value)))

case class QueueDefinition(name: String, callback: QueueTask.Callback, taskContext: TaskContext = new TaskContext,
                           priorities: Iterable[Priority] = Seq(Priority(1, weight = 1)))

sealed trait QueueItem

object QueueItem {

  case class Task(taskId: Timestamp, token: Long, priority: Int, data: Any) extends QueueItem

  case class Ack(ackId: Timestamp, taskId: Timestamp) extends QueueItem

  implicit def item2data(item: QueueItem.Task): FeederData = {
    Map("token" -> item.token, "id" -> item.taskId.value, "priority" -> item.priority, "data" -> item.data)
  }

  implicit val TaskOrdering = new Ordering[QueueItem.Task] {
    def compare(x: QueueItem.Task, y: QueueItem.Task) = x.taskId.compareTo(y.taskId)
  }
}

trait QueueStats {
  def totalTasks: Int

  def pendingTasks: Iterable[QueueItem.Task]

  def delayedTasks: Iterable[QueueItem.Task]
}

trait Queue {

  def token: Long

  def definition: QueueDefinition

  def name: String = definition.name

  def priorities: Iterable[Priority] = definition.priorities

  def enqueue(taskItem: QueueItem.Task)

  def ack(ackItem: QueueItem.Ack)

  def feeder: Feeder

  def stats: QueueStats

  def start()

  def stop()
}

object Queue {
  type QueueFactory = (Long, QueueDefinition, Service with QueueService) => Queue
}

object QueueTask {
  type Data = Any
  type Callback = (Data) => Future[Result]

  sealed trait Result

  object Result {

    object Ok extends Result

    case class Fail(error: Exception, ignore: Boolean = false) extends Result

  }

}
