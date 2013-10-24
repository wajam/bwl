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

  private def task(taskId: Long, priority: Int = 1) = QueueItem.Task(taskId, token = taskId, priority, data = taskId)

  private def ack(ackId: Long, taskId: Long) = QueueItem.Ack(ackId, taskId, token = taskId)

  trait QueueService extends MockitoSugar {
    val member = new ServiceMember(0, new LocalNode(Map("nrv" -> 34578)))
    val service = new Service("queue") {
      override def getMemberAtToken(token: Long) = Some(member)

      override def getMemberTokenRanges(member: ServiceMember) = List(TokenRange.All)

      override def consistency = new ConsistencyOne()

      override def responseTimeout = 1000L
    }
    val priorities = List(Priority(1, weight = 66), Priority(2, weight = 33))

    val definition: QueueDefinition = QueueDefinition("name", (_) => mock[Future[QueueTask.Result]], priorities = priorities)

    val resource = new QueueResource((_, _) => None, (_) => definition, (_) => member)
    resource.registerTo(service)

    // Execute specified test with a log queue factory. The test can create multiple queue instances but must stop
    // using the previously created instance. All queue instances are backed by the same log files.
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

    def waitForFeederData(feeder: Feeder, timeoutInMs: Long = 2000L, sleepTimeInMs: Long = 50L) {
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

      // ####################
      // Create a brand new empty queue
      val queue1 = createQueue()

      // Verification before enqueue
      queue1.feeder.take(20).flatten.toList should be(Nil)
      queue1.stats.verifyEqualsTo(totalTasks = 0, pendingTasks = Nil)

      // Enqueue out of order
      val t3 = queue1.enqueue(task(taskId = 3, priority = 1))
      val t1 = queue1.enqueue(task(taskId = 1, priority = 1))
      val t2 = queue1.enqueue(task(taskId = 2, priority = 1))
      waitForFeederData(queue1.feeder)

      // Verification after enqueue
      queue1.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)
      queue1.feeder.take(20).flatten.toList should be(List(t1, t2, t3).map(_.toFeederData))
      queue1.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = List(t1, t2, t3))
      queue1.stop()

      // ####################
      // Create a new queue instance without acknowledging any tasks. Should reload the same state.
      val queue2 = createQueue()
      waitForFeederData(queue2.feeder)
      queue2.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)
      queue2.feeder.take(20).flatten.toList should be(List(t1, t2, t3).map(_.toFeederData))
      queue2.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = List(t1, t2, t3))

      // Ack first task (queue + feeder)
      queue2.ack(ack(4, taskId = t1.taskId.value))
      queue2.feeder.ack(t1.toFeederData)
      queue2.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = List(t2, t3))
      queue2.stop()

      // ####################
      // Create a new queue instance again. Should reload the state after first task ack.
      val queue3 = createQueue()
      waitForFeederData(queue3.feeder)
      queue3.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = Nil)
      queue3.feeder.take(20).flatten.toList should be(List(t2, t3).map(_.toFeederData))
      queue3.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = List(t2, t3))
    })
  }
}
