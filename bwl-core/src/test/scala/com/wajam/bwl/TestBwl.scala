package com.wajam.bwl

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.bwl.queue._
import scala.concurrent.{ Future, Await, ExecutionContext }
import scala.concurrent.duration._
import org.scalatest.matchers.ShouldMatchers._
import org.mockito.Matchers.{ eq => argEquals }
import org.mockito.Mockito._
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestBwl extends FunSuite {

  import BwlFixture._

  def singlePriorityQueue(implicit queueFactory: QueueFactory) {

    test(queueFactory.name + " - queue should enqueue and dequeue expected value") {
      new OkCallbackFixture with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "hello")
        verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))
        verifyNoMoreInteractions(f.mockCallback)
      })
    }

  }

  def multiplePrioritiesQueue(implicit queueFactory: QueueFactory) {

    test(queueFactory.name + " - enqueued priority weights should be respected") {
      import ExecutionContext.Implicits.global

      new OkCallbackFixture with BwlFixture with MultiplePriorityQueueFixture {}.runWithFixture((f) => {
        val enqueued = f.definitions.head.priorities.flatMap(p =>
          1.to(200).map(i => {
            f.bwl.enqueue(i, f.definitions.head.name, Map("p" -> p.value, "i" -> i), Some(p.value))
          }))

        Await.result(Future.sequence(enqueued), 5.seconds)

        val dataCaptor = ArgumentCaptor.forClass(classOf[Map[String, Any]])
        verify(f.mockCallback, timeout(5000).atLeast(100)).process(dataCaptor.capture())

        val values = dataCaptor.getAllValues.toList.take(100)
        val p1Count = values.map(_("p")).count(_ == 1)
        val p2Count = values.map(_("p")).count(_ == 2)

        p1Count should (be > 55 and be < 75)
        p2Count should (be > 25 and be < 45)
      })
    }

  }

  testsFor(multiplePrioritiesQueue(memoryQueueFactory))
  testsFor(singlePriorityQueue(memoryQueueFactory))
  testsFor(multiplePrioritiesQueue(persistentQueueFactory))
  testsFor(singlePriorityQueue(persistentQueueFactory))

  test("ok callback should BE akcnowledged") {
    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new OkCallbackFixture with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(1)
    })
  }

  test("fail callback should NOT BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new FailCallbackFixture() with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      Thread.sleep(100)
      verify(spyQueue, never()).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(0)
    })
  }

  test("fail ignore callback should BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new FailCallbackFixture(ignore = true) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(1)
    })
  }

  test("ok callback completed after response timeout should NOT BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new OkCallbackFixture(delay = 300L) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.applySupport(responseTimeout = Some(200L))

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      // Sleep to give enough time to timeout
      Thread.sleep(500)
      verify(spyQueue, never()).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(0)
    })
  }

  test("fail ignore callback completed after response timeout should NOT BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new FailCallbackFixture(delay = 300L, ignore = true) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.applySupport(responseTimeout = Some(200L))

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      // Sleep to give enough time to timeout
      Thread.sleep(500)
      verify(spyQueue, never()).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(0)
    })
  }

}
