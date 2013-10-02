package com.wajam.bwl

import org.scalatest.{ BeforeAndAfter, FunSuite }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{ LocalNode, StaticClusterManager, Cluster }
import com.wajam.nrv.protocol.NrvProtocol
import com.wajam.bwl.queue.{ QueueTask, Priority, QueueDefinition }
import com.wajam.bwl.queue.memory.MemoryQueue
import scala.concurrent.{ Future, Await, ExecutionContext }
import scala.concurrent.duration._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.spnl.{ TaskContext, Spnl }
import org.mockito.Matchers.{ eq => argEquals }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestBwl extends FunSuite with BeforeAndAfter with MockitoSugar {

  private var bwl: Bwl = null
  private var cluster: Cluster = null

  private var mockCallback: Callback = null
  private var single: QueueDefinition = null
  private var weighted: QueueDefinition = null

  before {
    val manager = new StaticClusterManager
    cluster = new Cluster(new LocalNode("localhost", Map("nrv" -> 40373)), manager)

    val protocol = new NrvProtocol(cluster.localNode, 5000, 100)
    cluster.registerProtocol(protocol, default = true)

    mockCallback = mock[Callback]
    single = QueueDefinition("single", okCallback(mockCallback), createTaskContext)
    weighted = QueueDefinition("weighted", okCallback(mockCallback), createTaskContext,
      priorities = List(Priority(1, weight = 33), Priority(2, weight = 66)))

    bwl = new Bwl(definitions = List(single, weighted), createQueue = MemoryQueue.create, spnl = new Spnl)
    bwl.applySupport(responseTimeout = Some(2000))
    cluster.registerService(bwl)
    bwl.addMember(0, cluster.localNode)

    cluster.start()
  }

  after {
    cluster.stop()
  }

  private def createTaskContext = new TaskContext(normalRate = 50, throttleRate = 50)

  private trait Callback {
    def process(data: QueueTask.Data)
  }

  private def okCallback(callback: Callback)(data: QueueTask.Data): Future[QueueTask.Result] = {
    import scala.concurrent.future
    import ExecutionContext.Implicits.global

    future {
      callback.process(data)
      QueueTask.Result.Ok
    }
  }

  test("should enqueue and dequeue expected value") {
    import ExecutionContext.Implicits.global

    val t = bwl.enqueue(0, single.name, "hello")
    val id = Await.result(t, Duration.Inf)

    val expectedData = Map("token" -> "0", "id" -> id, "data" -> "hello")
    verify(mockCallback, timeout(200)).process(argEquals(expectedData))
    verifyNoMoreInteractions(mockCallback)
  }

  test("enqueued priority weights should be respected") {
    import ExecutionContext.Implicits.global

    val enqueued = weighted.priorities.flatMap(p => 1.to(200).map(i =>
      bwl.enqueue(i, weighted.name, p.value, Some(p.value))))
    //  bwl.enqueue(i, double.name, Map("p" -> p.value, "i" -> i), Some(p.value))))
    Await.result(Future.sequence(enqueued), 2 seconds)

    val dataCaptor = ArgumentCaptor.forClass(classOf[Map[String, Any]])
    verify(mockCallback, timeout(2000).atLeast(100)).process(dataCaptor.capture())
    val values = dataCaptor.getAllValues.toList

    val p1Count = values.map(_("data")).count(_ == 1)
    val p2Count = values.map(_("data")).count(_ == 2)
    p2Count should be > p1Count
    println(s"1=$p1Count, 2=$p2Count")
  }

}
