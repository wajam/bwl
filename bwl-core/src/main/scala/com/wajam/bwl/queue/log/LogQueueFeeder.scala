package com.wajam.bwl.queue.log

import com.wajam.spnl.feeder.Feeder
import com.wajam.bwl.queue.QueueItem
import scala.collection.immutable.TreeMap
import com.wajam.spnl.TaskContext
import com.wajam.bwl.queue.{ QueueDefinition, PrioritySelector }
import com.wajam.bwl.utils.{ FeederPositionTracker, PeekIterator }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.QueueResource._
import com.wajam.spnl.feeder.Feeder._
import scala.util.Random

/**
 * Feeder implementation for the LogQueue persistent queue.
 */
class LogQueueFeeder(definition: QueueDefinition, createPriorityReader: (Int, Option[Timestamp]) => PriorityTaskItemReader)(implicit random: Random = Random)
    extends Feeder {

  private val selector = new PrioritySelector(definition.priorities)
  private var readers: Map[Int, PriorityTaskItemReader] = Map()
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

  def peek(): Option[FeederData] = {
    if (randomTaskIterator.nonEmpty) {
      randomTaskIterator.peek match {
        case None => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case Some(item) => Some(item.toFeederData)
      }
    } else {
      None
    }
  }

  def next(): Option[FeederData] = {
    if (randomTaskIterator.nonEmpty) {
      randomTaskIterator.next() match {
        case Some(item) => {
          trackers(item.priority) += item.taskId
          pendingItems += item.taskId -> item
          Some(item.toFeederData)
        }
        case None => None
      }
    } else {
      None
    }
  }

  def ack(data: FeederData) {
    val priority = data(TaskPriority).toString.toInt
    val taskId = data(TaskId).toString.toLong
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
    position.foreach(taskContext.data += priority.toString -> _.value)
  }

  def kill() {
    readers.valuesIterator.foreach(_.close())
  }

  def pendingTaskPriorityFor(taskId: Timestamp): Option[Int] = pendingItems.get(taskId).map(_.priority)

  def pendingTasks: Iterator[QueueItem.Task] = pendingItems.valuesIterator

  // TODO: Should delayed tasks be sorted by id or we don't really care?
  def delayedTasks: Iterator[QueueItem.Task] = readers.valuesIterator.flatMap(_.delayedTasks)
}