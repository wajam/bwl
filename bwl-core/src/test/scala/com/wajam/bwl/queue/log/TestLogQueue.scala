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
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.commons.Closable.using

@RunWith(classOf[JUnitRunner])
class TestLogQueue extends FlatSpec {

  private def task(taskId: Long, priority: Int = 1) = QueueItem.Task("name", token = taskId, priority, taskId, data = taskId)

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
    def withQueueFactory(test: (() => LogQueue) => Any) {
      var queues: List[Queue] = Nil
      val dataDir = Files.createTempDirectory("TestLogQueue").toFile
      try {

        def createQueue: LogQueue = {
          val queue = LogQueue.create(dataDir)(token = 0, definition, service)
          queues = queue :: queues
          queue.start()
          queue.feeder.init(definition.taskContext)
          queue.asInstanceOf[LogQueue]
        }

        test(createQueue _)
      } finally {
        queues.foreach(_.stop())
        FileUtils.deleteDirectory(dataDir)
      }
    }

    /**
     * Wait until specified feeder is ready to produce non empty data or the timeout is reach.
     */
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
      val t3 = queue1.enqueue(task(taskId = 3L, priority = 1))
      val t1 = queue1.enqueue(task(taskId = 1L, priority = 1))
      val t2 = queue1.enqueue(task(taskId = 2L, priority = 1))
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
      queue2.ack(t1.toAck(ackId = 4L))
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

  it should "return last queue item identifier" in new QueueService {
    withQueueFactory(createQueue => {
      val queue1 = createQueue()

      // Empty queue
      queue1.getLastQueueItemId should be(None)

      // Non-empty queue
      queue1.enqueue(task(taskId = 3L, priority = 1))
      queue1.enqueue(task(taskId = 1L, priority = 2))
      queue1.enqueue(task(taskId = 4L, priority = 2))
      queue1.enqueue(task(taskId = 5L, priority = 1))
      queue1.enqueue(task(taskId = 2L, priority = 1))
      waitForFeederData(queue1.feeder)
      queue1.getLastQueueItemId should be(Some(Timestamp(5L)))
      queue1.stop()

      // New non-empty queue instance
      val queue2 = createQueue()
      queue2.getLastQueueItemId should be(Some(Timestamp(5L)))
    })
  }

  it should "read queue items ordered by id with mixed priorities" in new QueueService {
    withQueueFactory(createQueue => {
      val queue1 = createQueue()

      val t3 = queue1.enqueue(task(taskId = 3L, priority = 1))
      val t1 = queue1.enqueue(task(taskId = 1L, priority = 2))
      val t4 = queue1.enqueue(task(taskId = 4L, priority = 2))
      val t2 = queue1.enqueue(task(taskId = 2L, priority = 1))
      waitForFeederData(queue1.feeder)
      queue1.feeder.take(20).toList // Load all items with feeder before acknowledging them

      val a8_t2 = queue1.ack(t2.toAck(8L))
      val a5_t1 = queue1.ack(t1.toAck(5L))
      val a7_t3 = queue1.ack(t3.toAck(7L))
      val a6_t4 = queue1.ack(t4.toAck(6L))
      val t9 = queue1.enqueue(task(taskId = 9L, priority = 2))
      waitForFeederData(queue1.feeder)

      val readItems = using(queue1.readQueueItems(startItemId = t3.taskId, endItemId = a7_t3.ackId)) { reader =>
        reader.toList
      }
      readItems should be(List(t3, t4, a5_t1, a6_t4, a7_t3))
    })
  }

  it should "read queue items ordered by id and one priority is empty" in new QueueService {
    withQueueFactory(createQueue => {
      val queue1 = createQueue()

      val t3 = queue1.enqueue(task(taskId = 3L, priority = 1))
      val t1 = queue1.enqueue(task(taskId = 1L, priority = 1))
      val t4 = queue1.enqueue(task(taskId = 4L, priority = 1))
      val t2 = queue1.enqueue(task(taskId = 2L, priority = 1))
      waitForFeederData(queue1.feeder)
      queue1.feeder.take(20).toList // Load all items with feeder before acknowledging them

      val a8_t2 = queue1.ack(t2.toAck(8L))
      val a5_t1 = queue1.ack(t1.toAck(5L))
      val a7_t3 = queue1.ack(t3.toAck(7L))
      val a6_t4 = queue1.ack(t4.toAck(6L))
      val t9 = queue1.enqueue(task(taskId = 9L, priority = 1))
      waitForFeederData(queue1.feeder)

      val readItems = using(queue1.readQueueItems(startItemId = t3.taskId, endItemId = a7_t3.ackId)) { reader =>
        reader.toList
      }
      readItems should be(List(t3, t4, a5_t1, a6_t4, a7_t3))
    })
  }

  it should "read queue items queued in different log files" in new QueueService {
    withQueueFactory(createQueue => {

      // Simulate file rolling by adding each task item in a new queue instance,
      def enqueue(item: QueueItem.Task): QueueItem.Task = {
        val queue = createQueue()
        // Empty the feeder before writing the item so wait unblock when the new item is readable
        queue.feeder.take(20).toList
        queue.enqueue(item)
        waitForFeederData(queue.feeder)
        queue.stop()
        item
      }

      val t1 = enqueue(task(taskId = 1L, priority = 1))
      val t2 = enqueue(task(taskId = 2L, priority = 1))
      val t3 = enqueue(task(taskId = 3L, priority = 1))
      val t4 = enqueue(task(taskId = 4L, priority = 1))

      val queue1 = createQueue()
      queue1.feeder.take(20).toList // Load all items with feeder before acknowledging them

      val a8_t2 = queue1.ack(t2.toAck(8L))
      val a5_t1 = queue1.ack(t1.toAck(5L))
      val a7_t3 = queue1.ack(t3.toAck(7L))
      val a6_t4 = queue1.ack(t4.toAck(6L))
      val t9 = enqueue(task(taskId = 9L, priority = 1))

      val readItems = using(createQueue().readQueueItems(startItemId = a6_t4.ackId, endItemId = t9.taskId)) { reader =>
        reader.toList
      }
      readItems should be(List(a6_t4, a7_t3, a8_t2, t9))
    })
  }

  it should "write queue items" in new QueueService {
    withQueueFactory(createQueue => {

      val t1 = task(taskId = 1L, priority = 1)
      val t2 = task(taskId = 2L, priority = 1)
      val t3 = task(taskId = 3L, priority = 2)
      val t4 = task(taskId = 4L, priority = 1)
      val a5_t1 = t1.toAck(5L)
      val a6_t4 = t4.toAck(6L)
      val a7_t3 = t3.toAck(7L)
      val a8_t2 = t2.toAck(8L)
      val t9 = task(taskId = 9L, priority = 1)

      val queue1 = createQueue()
      queue1.writeQueueItem(t1)
      queue1.writeQueueItem(t2)
      queue1.writeQueueItem(t3)
      queue1.writeQueueItem(t4)
      queue1.writeQueueItem(a5_t1)
      queue1.writeQueueItem(a6_t4)
      queue1.writeQueueItem(a7_t3)
      queue1.writeQueueItem(a8_t2)
      queue1.writeQueueItem(t9)
      waitForFeederData(queue1.feeder)

      val readItems = using(queue1.readQueueItems(startItemId = t3.taskId, endItemId = a7_t3.ackId)) { reader =>
        reader.toList
      }
      readItems should be(List(t3, t4, a5_t1, a6_t4, a7_t3))
    })
  }

  it should "returns expected oldest item" in new QueueService {
    withQueueFactory(createQueue => {
      val queue1 = createQueue()

      queue1.oldestItemIdFor(priority = 1) should be(None)
      queue1.oldestItemIdFor(priority = 2) should be(None)

      val t3 = queue1.enqueue(task(taskId = 3L, priority = 1))
      val t1 = queue1.enqueue(task(taskId = 1L, priority = 1))
      val t4 = queue1.enqueue(task(taskId = 4L, priority = 1))
      val t2 = queue1.enqueue(task(taskId = 2L, priority = 1))
      waitForFeederData(queue1.feeder)
      queue1.feeder.take(20).toList // Load all items with feeder before acknowledging them

      val a8_t2 = queue1.ack(t2.toAck(8L))
      queue1.feeder.ack(t2.toFeederData)
      queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(1L)))
      queue1.oldestItemIdFor(priority = 2) should be(None)

      val a5_t1 = queue1.ack(t1.toAck(5L))
      queue1.feeder.ack(t1.toFeederData)

      val a7_t3 = queue1.ack(t3.toAck(7L))
      queue1.feeder.ack(t3.toFeederData)

      val a6_t4 = queue1.ack(t4.toAck(6L))
      queue1.feeder.ack(t4.toFeederData)
      val t9 = queue1.enqueue(task(taskId = 9L, priority = 1))
      waitForFeederData(queue1.feeder)

      queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(4L)))
      queue1.oldestItemIdFor(priority = 2) should be(None)

      // Verify oldest respect the reader position
      using(queue1.readQueueItems(startItemId = t1.taskId, endItemId = t9.taskId)) { reader =>
        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(1L)))
        queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(1L)))
        reader.next() // Read one
        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(2L)))
        queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(2L)))
        reader.toList // Read to the end
        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(4L)))
        queue1.oldestItemIdFor(priority = 2) should be(None)
      }

      // Verify close revert oldest to pre-open values
      using(queue1.readQueueItems(startItemId = t1.taskId, endItemId = t9.taskId)) { reader =>
        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(1L)))
        queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(1L)))
        // Do not read to the end
      }
      queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(4L)))
      queue1.oldestItemIdFor(priority = 2) should be(None)

      // Verify multiple readers open concurrently
      using(queue1.readQueueItems(startItemId = t1.taskId, endItemId = t9.taskId)) { reader1 =>
        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(1L)))
        queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(1L)))
        reader1.next()
        reader1.next()
        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(3L)))
        queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(3L)))

        using(queue1.readQueueItems(startItemId = t1.taskId, endItemId = t9.taskId)) { reader2 =>
          queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(1L)))
          queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(1L)))
          reader2.next()
          queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(2L)))
          queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(2L)))
        }

        using(queue1.readQueueItems(startItemId = t2.taskId, endItemId = t9.taskId)) { reader3 =>
          queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(2L)))
          queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(2L)))
        }

        queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(3L)))
        queue1.oldestItemIdFor(priority = 2) should be(Some(Timestamp(3L)))
      }
      queue1.oldestItemIdFor(priority = 1) should be(Some(Timestamp(4L)))
      queue1.oldestItemIdFor(priority = 2) should be(None)
    })
  }

  it should "rewrite log tail if not properly finalized" in pending

}
