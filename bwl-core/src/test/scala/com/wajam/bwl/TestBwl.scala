package com.wajam.bwl

import org.scalatest.{ BeforeAndAfter, FunSuite }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{ LocalNode, StaticClusterManager, Cluster }
import com.wajam.nrv.protocol.NrvProtocol
import com.wajam.bwl.queue.{ Priority, QueueDefinition }
import com.wajam.bwl.queue.memory.MemoryQueue
import FeederTestHelper._
import scala.concurrent.{ Future, Await, ExecutionContext }
import scala.concurrent.duration._
import org.scalatest.matchers.ShouldMatchers._

@RunWith(classOf[JUnitRunner])
class TestBwl extends FunSuite with BeforeAndAfter {

  var bwl: Bwl = null
  var cluster: Cluster = null

  val single = QueueDefinition("single")
  val weighted = QueueDefinition("double", List(Priority(1, weight = 33), Priority(2, weight = 66)))

  before {
    val manager = new StaticClusterManager
    cluster = new Cluster(new LocalNode("localhost", Map("nrv" -> 40373)), manager)

    val protocol = new NrvProtocol(cluster.localNode, 5000, 100)
    cluster.registerProtocol(protocol, default = true)

    bwl = new Bwl(definitions = List(single, weighted), factory = MemoryQueue.create)
    bwl.applySupport(responseTimeout = Some(2000))
    cluster.registerService(bwl)
    bwl.addMember(0, cluster.localNode)

    cluster.start()
  }

  after {
    cluster.stop()
  }

  test("should enqueue and dequeue expected value") {
    import ExecutionContext.Implicits.global

    val t = bwl.enqueue(0, single.name, "hello")
    val id = Await.result(t, Duration.Inf)

    val feeder = bwl.createQueueFeeder(0, single.name).get
    val values = feeder.take(10).flatten.toList

    values.head("token") should be(0)
    values.head("id") should be(id)
    values.head("data") should be("hello")
  }

  test("enqueued priority weights should be respected") {
    import ExecutionContext.Implicits.global

    val enqueued = weighted.priorities.flatMap(p => 1.to(200).map(i =>
      bwl.enqueue(i, weighted.name, p.value, Some(p.value))))
    //  bwl.enqueue(i, double.name, Map("p" -> p.value, "i" -> i), Some(p.value))))
    Await.result(Future.sequence(enqueued), 2 seconds)

    val feeder = bwl.createQueueFeeder(0, weighted.name).get
    val values = feeder.take(100).flatten.toList

    val p1Count = values.map(_("data")).count(_ == 1)
    val p2Count = values.map(_("data")).count(_ == 2)
    p2Count should be > p1Count
    println(s"1=$p1Count, 2=$p2Count")
  }

}
