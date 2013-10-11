package com.wajam.bwl.queue.log

import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.{ TaskData, TaskContext }
import com.wajam.bwl.queue.{ QueueItem, QueueDefinition, PrioritySelector }
import com.wajam.bwl.utils.{ FeederPositionTracker, PeekIterator }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.QueueResource._
import scala.collection.immutable.TreeMap

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
  private var pendingItems: Map[Timestamp, QueueItem.Task] = TreeMap()

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

  def peek(): Option[TaskData] = {
    import QueueItem.item2data

    if (randomTaskIterator.nonEmpty) {
      randomTaskIterator.peek match {
        case None => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case Some(item) => Some(item2data(item))
      }
    } else {
      None
    }
  }

  def next(): Option[TaskData] = {
    import QueueItem.item2data

    randomTaskIterator.next() match {
      case Some(item) => {
        trackers(item.priority) += item.taskId
        pendingItems += item.taskId -> item
        Some(item2data(item))
      }
      case None => None
    }
  }

  def ack(data: TaskData) {
    val priority = data.values(TaskPriority).toString.toInt
    val taskId = data.values(TaskId).toString.toLong
    trackers(priority) -= taskId
    pendingItems -= taskId

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

  def pendingTaskPriorityFor(taskId: Timestamp): Option[Int] = pendingItems.get(taskId).map(_.priority)

  def pendingTasks: Iterator[QueueItem.Task] = pendingItems.valuesIterator

  def delayedTasks: Iterator[QueueItem.Task] = readers.valuesIterator.flatMap(_.delayedTasks)
}