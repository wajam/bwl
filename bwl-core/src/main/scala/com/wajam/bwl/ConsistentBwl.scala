package com.wajam.bwl

import com.wajam.nrv.consistency.ConsistentStore
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{ MValue, MessageType, InMessage, Message }
import com.wajam.nrv.service.{ ActionMethod, ServiceMember, TokenRange }
import com.wajam.bwl.queue.{ Queue, QueueItem, ConsistentQueue }
import com.wajam.nrv.extension.resource.ParamsAccessor
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.utils.{ ClosablePeekIterator, PeekIterator }
import com.wajam.commons.Closable
import com.wajam.nrv.utils.Startable

trait ConsistentBwl extends ConsistentStore with Startable {
  this: Bwl =>

  // Replica queues are queues replicated to this node.
  private var replicaQueues: Map[(Long, String), Queue] = Map()
  private var initializedReplicas: Set[Long] = Set()

  // Mapping between token ranges and service member to speedup reverse lookup.
  private var rangeMembers: Map[TokenRange, ServiceMember] = Map()

  private def updateRangeMemberCache() {
    rangeMembers = service.members.flatMap(member => service.getMemberTokenRanges(member).map((_, member))).toMap
  }

  /**
   * Returns true if the specified message is an enqueue or ack message referring to a consistent queue.
   */
  def requiresConsistency(message: Message) = {

    val pathIsMatching = message.method match {
      case ActionMethod.POST => queueResource.create(this).exists(_.matches(message.path, message.method))
      case ActionMethod.DELETE => queueResource.delete(this).exists(_.matches(message.path, message.method))
      case _ => false
    }

    if (pathIsMatching) {
      val params: ParamsAccessor = message

      val taskToken = params.param[Long](TaskToken)
      val member = resolveMembers(taskToken, 1).head
      val queueName = params.param[String](QueueName)

      // Ensure the message path is referring to a consistent queue
      queues.get((member.token, queueName)).exists(_.isInstanceOf[ConsistentQueue])
    } else {
      false
    }
  }

  /**
   * Returns the greatest queue item id from all the consistent queues matching the specified service member token ranges.
   */
  def getLastTimestamp(ranges: Seq[TokenRange]) = {
    val member = resolveMembers(range2token(ranges), 1).head
    initializeMemberReplicaQueues(member)
    allConsistentQueuesFor(member).flatMap(_.getLastQueueItemId).reduceOption(Ordering[Timestamp].max)
  }

  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp) = {
    // We don't have any background job that must not be executed beyond the service current consistent timestamp.
    // TODO: Should we delay tasks beyond the service consistent timestamp?
  }

  /**
   * Returns the enqueue and ack messages in id ascending order from all the consistent queues matching the
   * specified service member token ranges and id ranges.
   */
  def readTransactions(fromTime: Timestamp, toTime: Timestamp, ranges: Seq[TokenRange]) = {
    val memberToken = range2token(ranges)
    val consistentQueues = queues.map { case (key, wrapper) => key -> wrapper.queue }.collect {
      case ((queueToken, _), queue: ConsistentQueue) if queueToken == memberToken => queue
    }

    val iterators: List[PeekIterator[QueueItem] with Closable] = consistentQueues.map(queue =>
      ClosablePeekIterator(queue.readQueueItems(fromTime, toTime))).toList

    new Iterator[Message] with Closable {
      def hasNext = iterators.exists(_.hasNext)

      def next() = {
        // Peek the first value of all the queue iterators and select the smallest one (i.e. iterator with smallest head id)
        val nextItr: PeekIterator[QueueItem] = iterators.toIterator.withFilter(_.hasNext).reduce(QueueItem.IteratorOrdering.min)
        item2message(nextItr.next())
      }

      def close() = iterators.foreach(_.close())
    }
  }

  /**
   * Write the specified enqueue or ack message to the appropriate replica consistent queue
   */
  def writeTransaction(message: Message) = {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val member = resolveMembers(taskToken, 1).head
    val queueName = params.param[String](QueueName)

    initializeMemberReplicaQueues(member)
    replicaQueues.get((member.token, queueName)) match {
      case Some(queue: ConsistentQueue) => queue.writeQueueItem(message2item(message))
      case Some(queue: Queue) => throw new IllegalArgumentException(s"Queue '$queueName' is not a ConsistentQueue")
      case None => throw new IllegalArgumentException(s"Queue '$queueName' not found")
    }
  }

  def truncateAt(timestamp: Timestamp, token: Long) = {
    val member = resolveMembers(token, 1).head
    initializeMemberReplicaQueues(member)
    allConsistentQueuesFor(member).foreach(_.truncateQueueItem(timestamp))
  }

  override def start() = {
    super.start()

    updateRangeMemberCache()
  }

  private def allConsistentQueuesFor(member: ServiceMember): Iterator[ConsistentQueue] = {
    val allQueues = replicaQueues.toIterator ++ queues.toIterator.map { case (key, wrapper) => key -> wrapper.queue }
    allQueues.collect { case ((queueToken, _), queue: ConsistentQueue) if queueToken == member.token => queue }
  }

  /**
   * Ensure the replica consistent queues for the specified member are initialized or initialize them if needed.
   */
  private def initializeMemberReplicaQueues(member: ServiceMember) {
    if (!initializedReplicas.contains(member.token)) {
      synchronized {
        // TODO: Create only consistent queue once factory is in the queue definition
        replicaQueues ++= definitions.map(definition => (member.token, definition.name) -> createQueue(member.token, definition, this))
        initializedReplicas += member.token
      }
    }
  }

  private def range2token(ranges: Iterable[TokenRange]): Long = rangeMembers(ranges.head).token

  private def message2item(message: Message): QueueItem = {
    message.method match {
      case ActionMethod.POST if queueResource.create(this).exists(_.matches(message.path, message.method)) => {
        queueResource.message2task(message)
      }
      case ActionMethod.DELETE if queueResource.delete(this).exists(_.matches(message.path, message.method)) => {
        queueResource.message2ack(message)
      }
      case _ => throw new IllegalArgumentException(s"Unsupported BWL message: ${message.path}")
    }
  }

  private def item2message(item: QueueItem): Message = {
    item match {
      case taskItem: QueueItem.Task => task2message(taskItem)
      case ackItem: QueueItem.Ack => ack2message(ackItem)
    }
  }

  private def task2message(taskItem: QueueItem.Task): Message = {
    val params: Map[String, MValue] =
      Map(TaskId -> taskItem.taskId.toString, TaskToken -> taskItem.token.toString, TaskPriority -> taskItem.priority)
    val request = new InMessage(params, data = taskItem.data)
    request.token = taskItem.token
    request.timestamp = Some(taskItem.taskId)
    request.function = MessageType.FUNCTION_CALL
    request.serviceName = name
    request.method = ActionMethod.POST
    request.path = queueResource.create(this).get.path.buildPath(params)
    request
  }

  private def ack2message(ackItem: QueueItem.Ack): Message = {

    // TODO: Fix token. Must be added to QueueItem.Ack class
    val token: Long = 0 // ackItem.token

    val params: Map[String, MValue] = Map(TaskId -> ackItem.taskId.toString, TaskToken -> token.toString)
    val request = new InMessage(params)
    request.token = token
    request.timestamp = Some(ackItem.ackId)
    request.function = MessageType.FUNCTION_CALL
    request.serviceName = name
    request.method = ActionMethod.DELETE
    request.path = queueResource.delete(this).get.path.buildPath(params)
    request
  }
}