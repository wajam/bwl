package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import com.wajam.nrv.service.{ TokenRange, Service, ServiceMember }
import com.wajam.bwl.QueueResource
import com.wajam.nrv.cluster.LocalNode
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import com.wajam.bwl.queue._
import scala.concurrent.Future
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.consistency.ConsistencyOne
import com.wajam.bwl.FeederTestHelper._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.spnl.feeder.Feeder

@RunWith(classOf[JUnitRunner])
class TestLogQueue extends FlatSpec {

  def toTask(id: Long, priority: Int = 1): QueueItem.Task = QueueItem.Task(id, id, priority, id)

  trait QueueService extends MockitoSugar {
    val member = new ServiceMember(0, new LocalNode(Map("nrv" -> 34578)))
    val service = new Service("queue") {
      override def getMemberAtToken(token: Long) = Some(member)

      override def getMemberTokenRanges(member: ServiceMember) = List(TokenRange.All)

      override def consistency = new ConsistencyOne()

      override def responseTimeout = 1000L
    }
    val resource = new QueueResource((_, _) => None, (_) => member)
    resource.registerTo(service)

    val priorities = List(Priority(1, weight = 66), Priority(2, weight = 33))

    val definition: QueueDefinition = QueueDefinition("name", (_) => mock[Future[QueueTask.Result]], priorities = priorities)

    // Execute specified test with a log queue factory. The test can create multiple queue instances.
    // All queue instances are backed by the same log files but the test should only perform write operations
    // (enqueue, ack) on a single queue instance. The other queue instances can be used as snapshot which become
    // stale once the test perform write operation on a different queue.
    def withQueueFactory(test: (() => Queue) => Any) {
      var queues: List[Queue] = Nil
      val dataDir = Files.createTempDirectory("TestLogQueue").toFile
      try {

        def createQueue: Queue = {
          val queue = LogQueue.create(dataDir)(token = 0, definition, service)
          queues = queue :: queues
          queue.start()
          queue.feeder.init(definition.taskContext)
          queue
        }

        test(createQueue _)
      } finally {
        queues.foreach(_.stop())
        FileUtils.deleteDirectory(dataDir)
      }
    }

    def waitForFeederData(feeder: Feeder, timeoutInMs: Long = 1000L, sleepTimeInMs: Long = 50L) {
      val startTime = System.currentTimeMillis()
      while (feeder.peek().isEmpty) {
        val elapseTime = System.currentTimeMillis() - startTime
        if (elapseTime > timeoutInMs) {
          throw new RuntimeException(s"Timeout waiting for condition after $elapseTime ms.")
        }
        Thread.sleep(sleepTimeInMs)
      }
    }
  }

  /**
   * QueueStats wrapper which facilitate stats verification during the test
   */
  implicit class QueueStatsVerifier(stats: QueueStats) extends QueueStats {
    def totalTasks = stats.totalTasks
    def pendingTasks = stats.pendingTasks
    def delayedTasks = stats.delayedTasks

    def verifyEqualsTo(totalTasks: Int, pendingTasks: Iterable[QueueItem.Task] = Nil,
                       delayedTasks: Iterable[QueueItem.Task] = Nil) {
      stats.totalTasks should be(totalTasks)
      stats.pendingTasks.toList should be(pendingTasks.toList)
      stats.delayedTasks.toList should be(delayedTasks.toList)
    }
  }

  "Queue" should "enqueue and produce tasks" in new QueueService {
    withQueueFactory(createQueue => {
      val queue = createQueue()

      // Verification before enqueue
      queue.feeder.take(20).flatten.toList should be(Nil)
      queue.stats.verifyEqualsTo(totalTasks = 0, pendingTasks = Nil)

      // Enqueue out of order
      val t3 = queue.enqueue(toTask(id = 3, priority = 1))
      val t1 = queue.enqueue(toTask(id = 1, priority = 1))
      val t2 = queue.enqueue(toTask(id = 2, priority = 1))
      waitForFeederData(queue.feeder)

      // Verification after enqueue
      queue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)
      queue.feeder.take(20).flatten.toList should be(List(t1, t2, t3).map(_.toFeederData))
      queue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = List(t1, t2, t3))

      // Create a new queue instance without acknowledging any tasks. Should reload the same state.
      {
        val tempQueue = createQueue()
        waitForFeederData(tempQueue.feeder)
        tempQueue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)
        tempQueue.feeder.take(20).flatten.toList should be(List(t1, t2, t3).map(_.toFeederData))
        tempQueue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = List(t1, t2, t3))
      }

      // Ack first task (queue + feeder)
      queue.ack(QueueItem.Ack(4, taskId = t1.taskId))
      queue.feeder.ack(t1.toFeederData)
      queue.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = List(t2, t3))

      // Create a new queue instance again. Should reload the state after first task ack.
      {
        val tempQueue = createQueue()
        waitForFeederData(tempQueue.feeder)
        tempQueue.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = Nil)
        tempQueue.feeder.take(20).flatten.toList should be(List(t2, t3).map(_.toFeederData))
        tempQueue.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = List(t2, t3))
      }
    })
  }
}
