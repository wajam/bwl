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

  // Mapping between token ranges and service member to speedup lookup.
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
      queues.get((member.token, queueName)) match {
        case Some(QueueWrapper(_: ConsistentQueue, _)) => true
        case _ => false
      }
    } else {
      false
    }
  }

  /**
   * Returns the greatest queue item id from all the consistent queues matching the specified service member token ranges.
   */
  def getLastTimestamp(ranges: Seq[TokenRange]) = {
    val member = memberFor(range2token(ranges))
    initializeMemberReplicaQueues(member)
    allConsistentQueuesFor(member).flatMap(_.getLastQueueItemId).reduceOption(Ordering[Timestamp].max)
  }

  // TODO: Should we delay tasks beyond the service consistent timestamp?
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp) = {
    // Nothing to do here! We don't have any background job that must not be executed beyond the service
    // current consistent timestamp.
  }

  /**
   * Returns the enqueue and ack messages in id ascending order from all the consistent queues matching the
   * specified service member token ranges and id ranges.
   */
  def readTransactions(startTime: Timestamp, endTime: Timestamp, ranges: Seq[TokenRange]) = {
    val member = memberFor(range2token(ranges))
    val iterators: List[PeekIterator[QueueItem] with Closable] = allConsistentQueuesFor(member).map(queue =>
      ClosablePeekIterator(queue.readQueueItems(startTime, endTime))).toList

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
    val member = memberFor(taskToken)
    val queueName = params.param[String](QueueName)

    require(!queues.contains((member.token, queueName)),
      s"Cannot replicate to master replica queue ${member.token}:$queueName. Must be a slave replica queue!")

    initializeMemberReplicaQueues(member)
    replicaQueues.get((member.token, queueName)) match {
      case Some(queue: ConsistentQueue) => queue.writeQueueItem(message2item(message))
      case Some(queue: Queue) => throw new IllegalArgumentException(s"Queue '$queueName' is not a ConsistentQueue")
      case None => throw new IllegalArgumentException(s"Queue '$queueName' not found")
    }
  }

  def truncateAt(timestamp: Timestamp, token: Long) = {
    val member = memberFor(token)
    initializeMemberReplicaQueues(member)

    // There is no way to know in which queue the item to truncate is located. So call truncate on each possible queue.
    allConsistentQueuesFor(member).foreach(_.truncateQueueItem(timestamp))
  }

  abstract override def start() = {
    // TODO: Also update the cache when a shard is split (i.e. NewMemberAddedEvent)
    updateRangeMemberCache()

    super.start()
  }

  private def allConsistentQueuesFor(member: ServiceMember): Iterator[ConsistentQueue] = {
    val allQueues = replicaQueues.toIterator ++ queues.toIterator.map { case (key, wrapper) => key -> wrapper.queue }
    allQueues.collect { case ((queueToken, _), queue: ConsistentQueue) if queueToken == member.token => queue }
  }

  /**
   * Ensure the replica consistent queues for the specified member are initialized or initialize them if not.
   */
  private def initializeMemberReplicaQueues(member: ServiceMember) {
    if (!cluster.isLocalNode(member.node) && !initializedReplicas.contains(member.token)) {
      synchronized {
        // TODO: Create only consistent queues once factory is moved to the queue definition
        val newQueues = definitions.map(definition => queueFactory.createQueue(member.token, definition, this, instrumented = false))
        newQueues.foreach(_.start())
        replicaQueues ++= newQueues.map(queue => (member.token, queue.name) -> queue)
        initializedReplicas += member.token
      }
    }
  }

  private def memberFor(token: Long): ServiceMember = resolveMembers(token, 1).head

  private def range2token(ranges: Seq[TokenRange]): Long = {
    val members = ranges.collect(rangeMembers)
    val head = members.head
    require(members.forall(_ == head))
    head.token
  }

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

  def task2message(taskItem: QueueItem.Task): Message = {
    val params: Map[String, MValue] = Map(QueueName -> taskItem.name, TaskToken -> taskItem.token.toString,
      TaskPriority -> taskItem.priority, TaskId -> taskItem.taskId.toString)
    val request = new InMessage(params, data = taskItem.data)
    request.token = taskItem.token
    request.timestamp = Some(taskItem.taskId)
    request.function = MessageType.FUNCTION_CALL
    request.serviceName = name
    request.method = ActionMethod.POST
    request.path = queueResource.create(this).get.path.buildPath(params)
    request
  }

  def ack2message(ackItem: QueueItem.Ack): Message = {
    val params: Map[String, MValue] = Map(QueueName -> ackItem.name, TaskToken -> ackItem.token.toString,
      TaskPriority -> ackItem.priority, TaskId -> ackItem.taskId.toString)
    val request = new InMessage(params)
    request.token = ackItem.token
    request.timestamp = Some(ackItem.ackId)
    request.function = MessageType.FUNCTION_CALL
    request.serviceName = name
    request.method = ActionMethod.DELETE
    request.path = queueResource.delete(this).get.path.buildPath(params)
    request
  }
}