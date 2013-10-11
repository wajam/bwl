package com.wajam.bwl.queue.log

import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.{ TaskData, TaskContext }
import com.wajam.bwl.queue.{ QueueItem, QueueDefinition, PrioritySelector }
import com.wajam.bwl.utils.{ FeederPositionTracker, PeekIterator }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.QueueResource._

/**
 * Feeder implementation for the LogQueue persistent queue.
 */
class LogQueueFeeder(definition: QueueDefinition, createPriorityReader: (Int, Option[Timestamp]) => LogQueueReader)
    extends Feeder {

  private val selector = new PrioritySelector(definition.priorities)
  private var readers: Map[Int, LogQueueReader] = Map()
  private var randomTaskIterator: PeekIterator[Option[QueueItem.Task]] = PeekIterator(Iterator())

  // Keep track of non-acknowledged task and oldest task per priority
  private var trackers: Map[Int, FeederPositionTracker[Timestamp]] = Map()

  implicit def item2data(item: Option[QueueItem.Task]): Option[TaskData] = {
    item.map(d => TaskData(d.token,
      Map("token" -> d.token, "id" -> d.taskId.value, "priority" -> d.priority, "data" -> d.data)))
  }

  private var taskContext: TaskContext = null

  def name = definition.name

  def init(context: TaskContext) {
    taskContext = context

    // Initialize the position trackers (one per priority). The initial tracked position is loaded from the TaskContext
    trackers = definition.priorities.map(priority => {
      val initial = taskContext.data.get(priority.value.toString).map(value => Timestamp(value.toString.toLong))
      (priority.value, new FeederPositionTracker[Timestamp](initial))
    }).toMap

    // Initialize the priority readers (one per priority)
    readers = trackers.map {
      case (priority, tracker) => priority -> createPriorityReader(priority, tracker.oldestItemId)
    }.toMap
    randomTaskIterator = PeekIterator(Iterator.continually(readers(selector.next).next()))
  }

  def peek() = {
    if (randomTaskIterator.nonEmpty) {
      randomTaskIterator.peek match {
        case None => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case data => data
      }
    } else {
      None
    }
  }

  def next() = {
    randomTaskIterator.next() match {
      case Some(item) => {
        trackers(item.priority) += item.taskId
        item2data(Some(item))
      }
      case None => None
    }
  }

  def ack(data: TaskData) {
    val priority = data.values(TaskPriority).toString.toInt
    val taskId = data.values(TaskId).toString.toLong
    trackers(priority) -= taskId

    // Update task context with oldest processed or delayed item for the acknowledged item priority
    val oldestPendingTaskId = trackers(priority).oldestItemId
    val oldestDelayedTaskId = readers(priority).delayedTasks.headOption.map(_.taskId)
    val position: Option[Timestamp] = (oldestPendingTaskId, oldestDelayedTaskId) match {
      case (Some(pending), Some(delayed)) => Some(math.min(pending.value, delayed.value))
      case (id @ Some(_), None) => id
      case (None, id @ Some(_)) => id
      case (None, None) => None
    }
    position.foreach(taskContext.data += priority.toString -> _)
  }

  def kill() {
    readers.valuesIterator.foreach(_.close())
  }

  def pendingTaskPriorityFor(taskId: Timestamp): Option[Int] = trackers.collectFirst {
    case (priority, pendingIds) if pendingIds.contains(taskId) => priority
  }
}