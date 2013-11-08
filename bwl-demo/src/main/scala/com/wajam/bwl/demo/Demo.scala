package com.wajam.bwl.demo

import com.wajam.commons.Logging
import scala.concurrent.ExecutionContext
import com.wajam.nrv.cluster.{ StaticClusterManager, ClusterManager, Cluster, LocalNode }
import com.wajam.nrv.protocol._
import com.wajam.nrv.extension.resource._
import com.wajam.nrv.protocol.codec.StringCodec
import com.wajam.spnl.Spnl
import com.wajam.bwl.Bwl
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.bwl.queue.log.LogQueue
import java.io.File
import com.wajam.nrv.service.{ ActionSupportOptions, Service }
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.apache.log4j.PropertyConfigurator
import com.typesafe.config.ConfigFactory

object Demo extends App with Logging {

  import scala.concurrent.ExecutionContext.Implicits.global

  PropertyConfigurator.configureAndWatch("etc/log4j.properties", 5000)
  log.info("Bwl movement")

  val config: DemoConfig = new DemoConfig(ConfigFactory.load())
  val server = new DemoServer(config)
  sys.addShutdownHook(server.stop())
  server.startAndBlock()
}

class DemoServer(config: DemoConfig)(implicit ec: ExecutionContext) extends Logging {
  val ports = Map("nrv" -> config.getProtocolNrvPort, "http" -> config.getProtocolHttpPort)
  val node = new LocalNode(config.getNodeListenAddress, ports)

  log.info("Local node is {}", node)

  val httpProtocol = createHttpProtocol()
  val nrvProtocol = createNrvProtocol()

  val spnl = new Spnl()

  val definitions = List(QueueDefinition("single", new DemoResource.Callback("single")))

  val bwlService = new Bwl("bwl",
    definitions,
    LogQueue.create(new File(config.getBwlLogQueueDirectory)),
    spnl)

  val demoService = createService("demo")

  val cluster = new Cluster(node, createClusterManager(), new ActionSupportOptions(), Some(nrvProtocol))

  cluster.registerProtocol(httpProtocol)
  cluster.registerProtocol(nrvProtocol)
  demoService.applySupport(protocol = Some(nrvProtocol), supportedProtocols = Some(Set(httpProtocol)))

  cluster.registerService(demoService)
  cluster.registerService(bwlService)

  def start(): Unit = {
    cluster.start()
  }

  def startAndBlock(): Unit = {
    start()
    Thread.currentThread.join()
  }

  def stop(): Unit = {
    cluster.stop(config.getClusterShutdownTimeout)
  }

  private def createClusterManager(): ClusterManager = {
    config.getClusterManager match {
      case "static" => {
        val clusterManager = new StaticClusterManager
        clusterManager.addMembers(demoService, config.getDemoClusterMembers)
        clusterManager.addMembers(bwlService, config.getBwlClusterMembers)
        clusterManager
      }
      case "zookeeper" => {
        new ZookeeperClusterManager(new ZookeeperClient(config.getZookeeperServers))
      }
    }
  }

  private def createNrvProtocol(): Protocol = {
    val baseProtocol = new NrvProtocol(
      node,
      config.getProtocolNrvConnectionTimeoutMs,
      config.getProtocolConnectionPoolMaxSize)

    val localProtocol = new NrvMemoryProtocol(
      baseProtocol.name,
      node)

    new NrvLocalBranchingProtocol(baseProtocol.name, node, localProtocol, baseProtocol)
  }

  private def createHttpProtocol(): HttpProtocol = {
    val httpProtocol = new HttpProtocol("http",
      node,
      config.getProtocolHttpConnectionTimeoutMs,
      config.getProtocolHttpConnectionPoolMaxSize)
    httpProtocol.registerCodec("text/plain", new StringCodec)
    httpProtocol
  }

  private def createService(name: String): Service = {
    val service = new Service(name)
    val demoResource = new DemoResource(bwlService, definitions)
    service.registerResources(demoResource)
    service
  }

}
