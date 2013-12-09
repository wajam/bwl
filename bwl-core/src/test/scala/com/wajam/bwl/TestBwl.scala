package com.wajam.bwl

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.bwl.queue._
import scala.concurrent.{ Future, Await, ExecutionContext }
import scala.concurrent.duration._
import org.scalatest.matchers.ShouldMatchers._
import org.mockito.Matchers.{ eq => argEquals, anyObject }
import org.mockito.Mockito._
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TestBwl extends FunSuite {

  import BwlFixture._

  def singlePriorityQueue(implicit queueFactory: FixtureQueueFactory) {

    test(queueFactory.name + " - queue should enqueue and dequeue expected value") {
      new OkCallbackFixture with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "hello")
        verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))
        verifyNoMoreInteractions(f.mockCallback)
      })
    }

  }

  def multiplePrioritiesQueue(implicit queueFactory: FixtureQueueFactory) {

    test(queueFactory.name + " - enqueued priority weights should be respected") {
      import ExecutionContext.Implicits.global

      new OkCallbackFixture with BwlFixture with MultiplePriorityQueueFixture {}.runWithFixture((f) => {
        val enqueued = f.definitions.head.priorities.flatMap(p =>
          1.to(200).map(i => {
            f.bwl.enqueue(i, f.definitions.head.name, Map("p" -> p.value, "i" -> i), Some(p.value))
          }))

        Await.result(Future.sequence(enqueued), 5.seconds)

        val dataCaptor = ArgumentCaptor.forClass(classOf[Map[String, Any]])
        verify(f.mockCallback, timeout(5000).atLeast(100)).execute(dataCaptor.capture())

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

  test("ok callback should BE acknowledged") {
    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new OkCallbackFixture with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

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
      verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

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

  test("a zero queue callbackTimeout should override a non-zero responseTimeout") {
    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    // Set a meaningful callback delay
    val callbackDelay = 100L
    // Ensure timeout with a zero value
    val callbackTimeout = 0L

    new OkCallbackFixture(callbackDelay) with BwlFixture with SinglePriorityQueueFixture {
      override def definitions = super.definitions.map(_.copy(callbackTimeout = Some(callbackTimeout)))
    }.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head

      verify(spyQueue, timeout(500).never()).ack(anyObject())
    })
  }

  test("a non-zero queue callbackTimeout should override a zero responseTimeout") {
    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    // Set a meaningful callback delay
    val callbackDelay = 100L
    // Set a reasonable timeout at the queue level
    val callbackTimeout = 2000L

    new OkCallbackFixture(callbackDelay) with BwlFixture with SinglePriorityQueueFixture {
      override def definitions = super.definitions.map(_.copy(callbackTimeout = Some(callbackTimeout)))
    }.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      // Set a zero timeout at the service level
      f.bwl.applySupport(responseTimeout = Some(0))

      f.bwl.enqueue(0, f.definitions.head.name, "hello")
      verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(1)
    })
  }

  testsFor(callbackErrors(useResultException = false))
  testsFor(callbackErrors(useResultException = true))

  def callbackErrors(useResultException: Boolean) {

    val prefix = if (useResultException) "ResultException" else "Result"

    test(prefix + " - fail callback should NOT BE acknowledged") {

      implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

      val result = QueueCallback.Result.Fail(new Exception(), ignore = false)
      new ErrorCallbackFixture(result, useResultException) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "hello")
        verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

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

    test(prefix + " - fail ignore callback should BE acknowledged") {

      implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

      val result = QueueCallback.Result.Fail(new Exception(), ignore = true)
      new ErrorCallbackFixture(result, useResultException) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "hello")
        verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

        val spyQueue = spyQueueFactory.allQueues.head
        val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
        val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
        verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
        taskCaptor.getAllValues.size() should be(1)

        verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
        ackCaptor.getAllValues.size() should be(1)
      })
    }

    test(prefix + " - fail ignore callback completed after response timeout should NOT BE acknowledged") {

      implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

      val result = QueueCallback.Result.Fail(new Exception(), ignore = true)
      new ErrorCallbackFixture(result, useResultException, delay = 300L) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.applySupport(responseTimeout = Some(200L))

        f.bwl.enqueue(0, f.definitions.head.name, "hello")
        verify(f.mockCallback, timeout(2000)).execute(argEquals("hello"))

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

    test(prefix + " - fail callback should retry") {
      // Reduce SPNL retry delay with a Random that always returns 0
      implicit val random = new Random(new java.util.Random() {
        override def next(bits: Int) = 0
      })

      implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

      val result = QueueCallback.Result.Fail(new Exception(), ignore = false)
      new ErrorCallbackFixture(result, useResultException) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "hello")

        // SPNL would retry forever but we stop waiting after the fifth callback invocation
        verify(f.mockCallback, timeout(2000).atLeast(5)).execute(ArgumentCaptor.forClass(classOf[String]).capture())

        val spyQueue = spyQueueFactory.allQueues.head
        val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
        val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
        verify(spyQueue).enqueue(taskCaptor.capture())
        taskCaptor.getAllValues.size() should be(1)

        // Ensure the task is never acknowledged
        verify(spyQueue, never()).ack(ackCaptor.capture())
        ackCaptor.getAllValues.size() should be(0)
      })

    }

    test(prefix + " - fail callback should retry until max retry") {

      // Reduce SPNL retry delay with a Random that always returns 0
      implicit val random = new Random(new java.util.Random() {
        override def next(bits: Int) = 0
      })

      implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

      val maxRetryCount = 3

      val result = QueueCallback.Result.Fail(new Exception(), ignore = false)
      new ErrorCallbackFixture(result, useResultException) with BwlFixture with SinglePriorityQueueFixture {
        override def definitions = super.definitions.map(_.copy(maxRetryCount = Some(maxRetryCount)))
      }.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "hello")
        verify(f.mockCallback, timeout(2000).times(1 + maxRetryCount)).execute(argEquals("hello"))

        val spyQueue = spyQueueFactory.allQueues.head
        val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
        val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
        verify(spyQueue).enqueue(taskCaptor.capture())
        taskCaptor.getAllValues.size() should be(1)

        verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
        ackCaptor.getAllValues.size() should be(1)
      })
    }

    test(prefix + " - try later callback should trigger another enqueue") {
      val delay = 10000L
      val data = "hello"
      val priority = 1

      // Reduce SPNL retry delay with a Random that always returns 0
      implicit val random = new Random(new java.util.Random() {
        override def next(bits: Int) = 0
      })

      implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

      val result = QueueCallback.Result.TryLater(new Exception(), delay)
      new ErrorCallbackFixture(result, useResultException) with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, data, Some(priority))

        // Wait for the callback to be executed
        verify(f.mockCallback, timeout(2000)).execute(ArgumentCaptor.forClass(classOf[String]).capture())

        val spyQueue = spyQueueFactory.allQueues.head
        val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
        val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])

        // Ensure the task is enqueued again
        verify(spyQueue, timeout(2000).times(2)).enqueue(taskCaptor.capture())

        // Ensure the task is acknowledged
        verify(spyQueue).ack(ackCaptor.capture())

        // Get the last enqueued Task
        val tryLaterTask = taskCaptor.getAllValues.last

        // Ensure the new task is delayed
        tryLaterTask.scheduleTime should not be 'empty

        // Check task parameters and data
        tryLaterTask.data should be(data)
        tryLaterTask.name should be(f.definitions.head.name)
        tryLaterTask.priority should be(1)
        tryLaterTask.token should be(0)
      })
    }

    test(prefix + " - delayed tasks should be executed in order") {
      val delay = 1000L

      implicit val queueFactory = persistentQueueFactory

      new OkCallbackFixture with BwlFixture with SinglePriorityQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definitions.head.name, "good bye", delay = Some(delay))
        f.bwl.enqueue(1, f.definitions.head.name, "hello")

        val stringCaptor = ArgumentCaptor.forClass(classOf[String])

        verify(f.mockCallback, timeout(2000).times(2)).execute(stringCaptor.capture())

        stringCaptor.getAllValues.toList should be(List("hello", "good bye"))
      })
    }
  }
}
