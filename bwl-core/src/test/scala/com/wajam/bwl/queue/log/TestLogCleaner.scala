package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import com.wajam.nrv.consistency.log.{ LogRecord, FileTransactionLog }
import java.io.File
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import com.wajam.nrv.utils.timestamp.Timestamp
import org.scalatest.Matchers._
import com.wajam.nrv.data.{ OutMessage, Message, MessageType, InMessage }
import com.wajam.nrv.consistency.log.LogRecord.Index
import scala.util.Random
import com.wajam.commons.ControlableCurrentTime
import com.wajam.spnl.feeder.Feeder
import scala.concurrent.ExecutionContext
import com.wajam.bwl.utils.{ LogCleanerMetrics, DisabledMetrics }

@RunWith(classOf[JUnitRunner])
class TestLogCleaner extends FlatSpec {

  trait FileLog {
    implicit val metrics = new LogCleanerMetrics with DisabledMetrics

    def withTransactionLog(test: FileTransactionLog => Any) {
      val logDir: File = Files.createTempDirectory("TestLogCleaner").toFile
      val fileRolloverSize = 50 // Use a very small roll size to force file roll between each log record
      val txLog = new FileTransactionLog("0:queue#1", token = 0L, logDir.getAbsolutePath, fileRolloverSize,
        skipIntervalSize = Int.MaxValue, serializer = None)

      try {
        test(txLog)
      } finally {
        FileUtils.deleteDirectory(logDir)
      }
    }

    def createRequestMessage(timestamp: Long, token: Long = 0, data: Any = null): InMessage = {
      val request = new InMessage(Map("ts" -> timestamp, "tk" -> token), data = data)
      request.function = MessageType.FUNCTION_CALL
      request.token = token
      request.timestamp = Some(timestamp)
      request
    }

    def createResponseMessage(request: Message, code: Int = 200, error: Option[Exception] = None) = {
      val response = new OutMessage(code = code)
      request.copyTo(response)
      response.function = MessageType.FUNCTION_RESPONSE
      response.error = error
      response
    }

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

  "Cleaner" should "not crash if log is empty" in new FileLog {
    import ExecutionContext.Implicits.global
    withTransactionLog(txLog => {
      var oldestTimestamp: Option[Timestamp] = None
      val cleaner = new LogCleaner(txLog, oldestTimestamp, cleanFrequencyInMs = 1000000L)
      cleaner.cleanupLogFiles() should be(Nil)

      oldestTimestamp = Some(0L)
      cleaner.cleanupLogFiles() should be(Nil)
    })
  }

  it should "clean expected files" in new FileLog {
    import ExecutionContext.Implicits.global
    withTransactionLog(txLog => {

      var oldestTimestamp: Option[Timestamp] = None
      val cleaner = new LogCleaner(txLog, oldestTimestamp, cleanFrequencyInMs = 1000000L)

      txLog.getLogFiles.size should be(0)
      val t1 = txLog.append(LogRecord(id = 100, None, createRequestMessage(timestamp = 1)))
      val i1 = txLog.append(Index(id = 110, Some(1L)))
      val t2 = txLog.append(LogRecord(id = 120, Some(1L), createRequestMessage(timestamp = 2)))
      val i2 = txLog.append(Index(id = 130, Some(2L)))
      val t3 = txLog.append(LogRecord(id = 140, Some(2L), createRequestMessage(timestamp = 3)))
      val i3 = txLog.append(Index(id = 150, Some(3L)))
      val t4 = txLog.append(LogRecord(id = 160, Some(3L), createRequestMessage(timestamp = 4)))
      val i4 = txLog.append(Index(id = 170, Some(4L)))
      val t5 = txLog.append(LogRecord(id = 180, Some(4L), createRequestMessage(timestamp = 5)))
      val i5 = txLog.append(Index(id = 190, Some(5L)))
      txLog.getLogFiles.size should be(10)

      cleaner.cleanupLogFiles() should be(Nil)
      txLog.getLogFiles.size should be(10)

      oldestTimestamp = Some(0L)
      cleaner.cleanupLogFiles() should be(Nil)
      txLog.getLogFiles.size should be(10)

      oldestTimestamp = Some(1L)
      cleaner.cleanupLogFiles() should be(Nil)
      txLog.getLogFiles.size should be(10)

      oldestTimestamp = Some(2L)
      cleaner.cleanupLogFiles() should be(Nil)
      txLog.getLogFiles.size should be(10)

      oldestTimestamp = Some(3L)
      cleaner.cleanupLogFiles().size should be(2)
      txLog.getLogFiles.size should be(8)
      txLog.read.toList should be(List(t2, i2, t3, i3, t4, i4, t5, i5))

      oldestTimestamp = Some(5L)
      cleaner.cleanupLogFiles().size should be(4)
      txLog.getLogFiles.size should be(4)
      txLog.read.toList should be(List(t4, i4, t5, i5))
    })
  }

  it should "trigger cleanup on tick" in new FileLog {
    import ExecutionContext.Implicits.global
    withTransactionLog(txLog => {

      implicit val random = new Random(9) // Tick will trigger cleanup after 120ms
      var oldestTimestamp: Option[Timestamp] = None
      val cleaner = new LogCleaner(txLog, oldestTimestamp, cleanFrequencyInMs = 200) with ControlableCurrentTime

      txLog.getLogFiles.size should be(0)
      val t1 = txLog.append(LogRecord(id = 100, None, createRequestMessage(timestamp = 1)))
      val t2 = txLog.append(LogRecord(id = 120, Some(1L), createRequestMessage(timestamp = 2)))
      val t3 = txLog.append(LogRecord(id = 140, Some(2L), createRequestMessage(timestamp = 3)))
      val t4 = txLog.append(LogRecord(id = 160, Some(3L), createRequestMessage(timestamp = 4)))
      val t5 = txLog.append(LogRecord(id = 180, Some(4L), createRequestMessage(timestamp = 5)))
      txLog.getLogFiles.size should be(5)

      oldestTimestamp = Some(4L)
      cleaner.currentTime = 120
      cleaner.tick() should be(true)
      waitForCondition() {
        // Wait until cleanup completed or timeout
        txLog.getLogFiles.size == 3
      }
      txLog.read.toList should be(List(t3, t4, t5))
    })
  }

  it should "tick tick boom once per frequency period" in new FileLog {
    import ExecutionContext.Implicits.global
    withTransactionLog(txLog => {

      implicit val random = new Random(9) // Tick will trigger cleanup after 120ms
      var oldestTimestamp: Option[Timestamp] = None
      val cleaner = new LogCleaner(txLog, oldestTimestamp, cleanFrequencyInMs = 200) with ControlableCurrentTime
      cleaner.currentTime = 0
      cleaner.tick() should be(false)
      cleaner.currentTime = 119
      cleaner.tick() should be(false)
      cleaner.currentTime = 120
      cleaner.tick() should be(true)
      cleaner.tick() should be(false)
      cleaner.currentTime = 319
      cleaner.tick() should be(false)
      cleaner.currentTime = 320
      waitForCondition() {
        // Previous cleanup may still be running, retry until it pass or timeout
        cleaner.tick()
      }
      cleaner.tick() should be(false)
    })
  }
}
