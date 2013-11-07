package com.wajam.bwl.demo

import com.typesafe.config.Config
import scala.collection.JavaConversions._

class DemoConfig(config: Config) {
  def getNodeListenAddress: String = config.getString("nrv.node.listen-address")
  def getClusterManager: String = config.getString("nrv.cluster.manager")
  def getClusterShutdownTimeout: Int = config.getInt("nrv.cluster.shutdown-timeout")
  def getZookeeperServers: String = config.getString("nrv.zookeeper.servers")

  def getProtocolNrvPort: Int = config.getInt("nrv.protocol.nrv.port")
  def getProtocolNrvConnectionTimeoutMs: Int = config.getInt("nrv.protocol.nrv.connection.timeout-ms")
  def getProtocolConnectionPoolMaxSize: Int = config.getInt("nrv.protocol.nrv.connection.pool.max-size")

  def getProtocolHttpPort: Int = config.getInt("nrv.protocol.http.port")
  def getProtocolHttpConnectionTimeoutMs: Int = config.getInt("nrv.protocol.http.connection.timeout-ms")
  def getProtocolHttpConnectionPoolMaxSize: Int = config.getInt("nrv.protocol.http.connection.pool.max-size")

  def getDemoClusterMembers: List[String] = config.getStringList("demo.cluster.members").toList
  def getBwlClusterMembers: List[String] = config.getStringList("bwl.cluster.members").toList
  def getBwlLogQueueDirectory: String = config.getString("bwl.log-queue.directory")
}
