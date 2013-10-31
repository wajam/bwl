package com.wajam.bwl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import scala.concurrent.ExecutionContext
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.bwl.queue.QueueItem
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.consistency.ResolvedServiceMember

@RunWith(classOf[JUnitRunner])
class TestConsistentBwl extends FlatSpec {

  import BwlFixture._

  "ConsistentBwl" should "requires consistency for task and ack messages for consistent queue" in {
    implicit val queueFactory: QueueFactory = persistentQueueFactory

    new OkCallbackFixture with ConsistentBwlFixture with MultiplePriorityQueueFixture {}.runWithConsistentFixture((f) => {
      val taskItem = QueueItem.Task(f.definitions.head.name, 1L, 1, 1L, "hello")

      val taskMsg = f.consistentBwl.task2message(taskItem)
      val ackMsg = f.consistentBwl.ack2message(taskItem.toAck(2L))

      f.consistentBwl.requiresConsistency(taskMsg) should be(true)
      f.consistentBwl.requiresConsistency(ackMsg) should be(true)

      // Not an enqueue or ack message
      val dummyMsg = new InMessage()
      f.consistentBwl.requiresConsistency(dummyMsg) should be(false)
    })
  }

  it should "NOT require consistency for task and ack messages for NON consistent queue" in {
    implicit val queueFactory: QueueFactory = memoryQueueFactory

    new OkCallbackFixture with ConsistentBwlFixture with MultiplePriorityQueueFixture {}.runWithConsistentFixture((f) => {
      val taskItem = QueueItem.Task(f.definitions.head.name, 1L, 1, 1L, "hello")

      val taskMsg = f.consistentBwl.task2message(taskItem)
      val ackMsg = f.consistentBwl.ack2message(taskItem.toAck(2L))

      f.consistentBwl.requiresConsistency(taskMsg) should be(false)
      f.consistentBwl.requiresConsistency(ackMsg) should be(false)
    })
  }

  it should "reads task and ack messages" in {
    implicit val queueFactory: QueueFactory = persistentQueueFactory
    import ExecutionContext.Implicits.global

    new OkCallbackFixture with ConsistentBwlFixture with MultipleQueuesFixture {}.runWithConsistentFixture((f) => {
      val ranges = ResolvedServiceMember(f.bwl, f.localMember).ranges

      // Empty bwl
      f.consistentBwl.getLastTimestamp(ranges) should be(None)
      f.consistentBwl.readTransactions(0L, Long.MaxValue, ranges).toList should be(Nil)

      // Enqueue a few items in the different queues
      val s = f.bwl.enqueue(0L, f.definitions(0).name, "single")
      val mp1 = f.bwl.enqueue(1L, f.definitions(1).name, "multiple-p1", priority = Some(1))
      val mp2 = f.bwl.enqueue(2L, f.definitions(1).name, "multiple-p2", priority = Some(2))

      val dataCaptor = ArgumentCaptor.forClass(classOf[String])
      verify(f.mockCallback, timeout(2000).times(3)).process(dataCaptor.capture())

      // When we are here, the 3 enqueue Tasks are written in the log and processed but we need to wait a bit more
      // to ensure their Ack are also written in the logs.
      Thread.sleep(500L)

      val transactions = f.consistentBwl.readTransactions(0L, Long.MaxValue, ranges).toList
      transactions.size should be(6)
      f.consistentBwl.getLastTimestamp(ranges) should be(transactions.last.timestamp)
    })
  }

  it should "write task and ack messages" in {
    implicit val queueFactory: QueueFactory = persistentQueueFactory

    new OkCallbackFixture with ConsistentBwlFixture with MultipleQueuesFixture {}.runWithConsistentFixture((f) => {

      val remoteToken = f.remoteMember.token

      val t1 = QueueItem.Task(f.definitions(0).name, token = remoteToken, 1, 1L, "hello")
      val t2 = QueueItem.Task(f.definitions(1).name, token = remoteToken, 2, 2L, "goodbye")
      val m1 = f.consistentBwl.task2message(t1)
      val m2 = f.consistentBwl.task2message(t2)
      val m3 = f.consistentBwl.ack2message(t2.toAck(3L))
      val m4 = f.consistentBwl.ack2message(t1.toAck(4L))

      // Write the items
      f.consistentBwl.writeTransaction(m1)
      f.consistentBwl.writeTransaction(m2)
      f.consistentBwl.writeTransaction(m3)
      f.consistentBwl.writeTransaction(m4)

      // Wait a bit to let the queue 'spit out' the consistent timestamp in its log
      Thread.sleep(500)

      val ranges = ResolvedServiceMember(f.bwl, f.remoteMember).ranges
      val transactions = f.consistentBwl.readTransactions(0L, m4.timestamp.get, ranges).toList
      transactions.map(_.timestamp) should be(List(m1, m2, m3, m4).map(_.timestamp))
      f.consistentBwl.getLastTimestamp(ranges) should be(m4.timestamp)
    })
  }

}
