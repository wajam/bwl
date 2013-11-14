package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import com.wajam.bwl.ClosableIterator._
import com.wajam.bwl.queue.QueueItem
import com.wajam.nrv.data.InMessage
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.nrv.consistency.replication.ReplicationSourceIterator

@RunWith(classOf[JUnitRunner])
class TestTaskItemReader extends FlatSpec {

  private def task(id: Int): QueueItem = QueueItem.Task("name", id, id, id, data = id)

  private def someTaskMessage(taskId: Int): Option[InMessage] = Some(LogQueue.item2request(task(taskId)))

  private def someAckMessage(ackId: Long, taskId: Int): Option[InMessage] = Some(
    LogQueue.item2request(QueueItem.Ack("name", token = taskId, priority = taskId, ackId, taskId)))

  "Reader" should "convert task request message to QueueItem.Task" in {
    val messages = Iterator(someTaskMessage(1), someTaskMessage(2))
    val reader = PriorityTaskItemReader(messages, Set())
    reader.toList should be(List(Some(task(1)), Some(task(2))))
  }

  it should "filter out processed tasks" in {
    val messages = Iterator(someTaskMessage(1), someTaskMessage(2), someTaskMessage(3))
    val reader = PriorityTaskItemReader(messages, processed = Set(2L))
    reader.toList should be(List(Some(task(1)), Some(task(3))))
  }

  it should "skip ack messages" in {
    val messages = Iterator(someTaskMessage(2), someTaskMessage(3), someAckMessage(4, taskId = 1))
    val reader = PriorityTaskItemReader(messages, Set())
    reader.toList should be(List(Some(task(2)), Some(task(3))))
  }

  it should "not filter out None" in {
    val messages = Iterator(None, someTaskMessage(1), None, None, someTaskMessage(2), someTaskMessage(3))
    val reader = PriorityTaskItemReader(messages, Set())
    reader.take(3).toList should be(List(None, Some(task(1)), None))
    reader.toList should be(List(None, Some(task(2)), Some(task(3))))
  }

  it should "call adapted Iterator.close() when closed" in new MockitoSugar {
    val mockItr = mock[ReplicationSourceIterator]
    val reader = PriorityTaskItemReader(mockItr, Set())
    reader.close()
    verify(mockItr).close()
  }

}
