package com.wajam.bwl.utils

import com.yammer.metrics.scala.{ Instrumented, Timer }
import com.wajam.bwl.queue.Queue
import com.wajam.bwl.queue.log.LogQueue

trait Instrumentable extends Instrumented {
  def scope: String
  def prioritiesList: List[Int]

  def instrument(f: => Unit): Unit = f
}

trait DisabledMetrics extends Instrumentable {
  override def instrument(f: => Unit) = Unit

  override val scope = ""
  override val prioritiesList = Nil
}

trait QueueMetrics extends DelayedTaskMetrics { this: Queue =>

  val scope = s"${name}-${token}"

  def prioritiesList: List[Int] = priorities.map(_.value).toList

  lazy val enqueuesMeter = metrics.meter("enqueues", "enqueues", scope)
  lazy val enqueueErrorsMeter = metrics.meter("enqueue-errors", "errors", scope)
  lazy val acksMeter = metrics.meter("acks", "acks", scope)
  lazy val ackErrorsMeter = metrics.meter("ack-errors", "errors", scope)

  instrument {
    val totalTasksGauge = metrics.gauge("total-tasks", scope) {
      stats.totalTasks
    }
    val processingTasksGauge = metrics.gauge("processing-tasks", scope) {
      stats.processingTasks.size
    }
    val delayedTasksGauge = metrics.gauge("delayed-tasks", scope) {
      stats.delayedTasks.size
    }
  }
}

trait LogQueueMetrics extends QueueMetrics with LogCleanerMetrics { this: LogQueue =>

  instrument {
    val truncatedTasksGauge = metrics.gauge("truncated-tasks-count", scope) {
      truncateTracker.count
    }
  }

  lazy val rebuildTimer = metrics.timer("rebuild-time", scope)
  lazy val rebuildActiveTasksCounter = metrics.counter("rebuild-active-tasks", scope)
  lazy val rebuildSkippedTasksCounter = metrics.counter("rebuild-skipped-tasks", scope)
}

trait DelayedTaskMetrics extends Instrumentable {
  lazy val taskWaitTimer = metrics.timer("task-wait-time", scope)
  lazy val taskDelayedTimer = metrics.timer("task-delayed-time", scope)

  lazy val dequeuesMeters = prioritiesList.map { p =>
    p -> metrics.meter(s"dequeues-$p", "dequeues", scope)
  }.toMap
}

trait LogCleanerMetrics extends Instrumentable {
  lazy val cleanupFileCounter = metrics.counter("cleanup-file-count", scope)
}
