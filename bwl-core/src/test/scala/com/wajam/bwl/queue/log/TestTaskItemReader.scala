package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import com.wajam.nrv.service.{ ServiceMember, Service }
import com.wajam.bwl.QueueResource
import com.wajam.bwl.ClosableIterator._
import com.wajam.nrv.cluster.LocalNode
import com.wajam.bwl.queue.QueueItem
import com.wajam.nrv.data.{ InMessage, Message }
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

@RunWith(classOf[JUnitRunner])
class TestTaskItemReader extends FlatSpec {

  trait QueueService {
    val service = new Service("queue")
    val resource = new QueueResource((_, _) => None, (_) => new ServiceMember(0, new LocalNode(Map("nrv" -> 34578))))
    resource.registerTo(service)
  }

  def toTask(id: Int): QueueItem = QueueItem.Task(id, id, id, id)

  def toSomeTaskMessage(taskId: Int): Option[InMessage] = Some(LogQueue.item2request(toTask(taskId)))

  def toSomeAckMessage(ackId: Long, taskId: Int): Option[InMessage] = Some(LogQueue.item2request(QueueItem.Ack(ackId, taskId)))

  "Reader" should "convert task request message to QueueItem.Task" in new QueueService {
    val messages = Iterator(toSomeTaskMessage(1), toSomeTaskMessage(2))
    val reader = PriorityTaskItemReader(service, messages, Set())
    reader.toList should be(List(Some(toTask(1)), Some(toTask(2))))
  }

  it should "filter out processed tasks" in new QueueService {
    val messages = Iterator(toSomeTaskMessage(1), toSomeTaskMessage(2), toSomeTaskMessage(3))
    val reader = PriorityTaskItemReader(service, messages, processed = Set(2L))
    reader.toList should be(List(Some(toTask(1)), Some(toTask(3))))
  }

  it should "skip ack messages" in new QueueService {
    val messages = Iterator(toSomeTaskMessage(2), toSomeTaskMessage(3), toSomeAckMessage(4, taskId = 1))
    val reader = PriorityTaskItemReader(service, messages, Set())
    reader.toList should be(List(Some(toTask(2)), Some(toTask(3))))
  }

  it should "not filter out None" in new QueueService {
    val messages = Iterator(None, toSomeTaskMessage(1), None, None, toSomeTaskMessage(2), toSomeTaskMessage(3))
    val reader = PriorityTaskItemReader(service, messages, Set())
    reader.take(3).toList should be(List(None, Some(toTask(1)), None))
    reader.toList should be(List(None, Some(toTask(2)), Some(toTask(3))))
  }

  it should "call adapted Iterator.close() when closed" in new QueueService with MockitoSugar {
    val mockItr = mock[Iterator[Option[Message]]]
    val reader = PriorityTaskItemReader(service, mockItr, Set())
    reader.close()
    verify(mockItr).close()
  }

}
