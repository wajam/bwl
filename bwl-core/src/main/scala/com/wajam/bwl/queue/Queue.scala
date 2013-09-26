package com.wajam.bwl.queue

import com.wajam.spnl.feeder.Feeder
import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.utils.WeightedItemsSelector

case class Priority(value: Int, weight: Int)

class PrioritySelector(priorities: Seq[Priority])
  extends WeightedItemsSelector(priorities.map(p => (p.weight.toDouble, p.value)))

case class QueueDefinition(name: String, priorities: Seq[Priority] = Seq(Priority(1, weight = 1)))

trait Queue {

  def token: Long

  def name: String

  def priorities: Seq[Priority]

  def enqueue(task: Message, priority: Int)

  def ack(id: Timestamp)

  def feeder: Feeder

  def start()

  def stop()
}

object Queue {
  type QueueFactory = (Long, QueueDefinition) => Queue
}
