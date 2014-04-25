package com.wajam.bwl

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable
import com.wajam.spnl.feeder.Feeder.FeederData
import language.implicitConversions
import com.wajam.bwl.queue.{ QueueItem, QueueStats }
import org.scalatest.Matchers._

object BwlTestHelper {
  def waitForCondition(timeoutInMs: Long = 2000L, sleepTimeInMs: Long = 50L)(predicate: => Boolean) {
    val startTime = System.currentTimeMillis()
    while (!predicate) {
      val elapseTime = System.currentTimeMillis() - startTime
      if (elapseTime > timeoutInMs) {
        throw new RuntimeException(s"Timeout waiting for condition after $elapseTime ms.")
      }
      Thread.sleep(sleepTimeInMs)
    }
  }
}

object FeederTestHelper {
  // TODO: Add in SPNL. This is a copy of a class also present in MRY
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[FeederData]] with Closable = {

    new Iterator[Option[FeederData]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }

  import BwlTestHelper._

  /**
   * Wait until specified feeder is ready to produce non empty data or the timeout is reach.
   */
  def waitForFeederData(feeder: Feeder, timeoutInMs: Long = 2000L, sleepTimeInMs: Long = 50L) {
    waitForCondition(timeoutInMs, sleepTimeInMs) {
      feeder.peek().nonEmpty
    }
  }
}

object QueueStatsHelper {
  /**
   * QueueStats wrapper which facilitate stats verification during the test
   */
  implicit class QueueStatsVerifier(stats: QueueStats) extends QueueStats {
    def totalTasks = stats.totalTasks

    def processingTasks = stats.processingTasks

    def delayedTasks = stats.delayedTasks

    def verifyEqualsTo(totalTasks: Int, processingTasks: Iterable[QueueItem.Task] = Nil,
                       delayedTasks: Iterable[QueueItem.Task] = Nil) {
      stats.totalTasks should be(totalTasks)
      stats.processingTasks.toList should be(processingTasks.toList)
      stats.delayedTasks.toList should be(delayedTasks.toList)
    }
  }
}