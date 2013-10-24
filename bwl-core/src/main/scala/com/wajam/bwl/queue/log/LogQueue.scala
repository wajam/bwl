package com.wajam.bwl.queue.log

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.data.MValue._
import java.io.File
import java.nio.file.{ Files, Paths }
import com.wajam.nrv.consistency._
import com.wajam.nrv.service.{ TokenRange, Service }
import com.wajam.nrv.consistency.log.{ TimestampedRecord, FileTransactionLog }
import com.wajam.nrv.consistency.replication.TransactionLogReplicationIterator
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random
import scala.annotation.tailrec
import com.wajam.nrv.consistency.log.LogRecord.Index
import com.wajam.commons.Logging
import LogQueue._

/**
 * Persistent queue using NRV transaction log as backing storage. Each priority is appended to separate log.
 */
class LogQueue(val token: Long, service: Service, val definition: QueueDefinition,
               recorderFactory: RecorderFactory)(implicit random: Random = Random) extends ConsistentQueue with Logging {

  private val recorders: Map[Int, TransactionRecorder] = priorities.map(p => p.value -> recorderFactory(token, definition, p)).toMap

  // Must not read tasks from logs beyond this position when rebuilding the initial state (rebuild is lazy and per priority),
  // By default the max rebuild position is the last item present in the log at the time the queue is created.
  // The state is also updated as new task are enqueued. The max rebuild position ensure that tasks enqueued before the
  // state is rebuilt are not computed twice. If the queue is empty and has no log, the max rebuild position is
  // initialized later at the first enqueued task.
  private var rebuildEndPositions: Map[Int, Timestamp] = recorders.collect {
    case (priority, recorder) if recorder.currentConsistentTimestamp.isDefined => priority -> recorder.currentConsistentTimestamp.get
  }.toMap

  private val totalTaskCount = new AtomicInteger()

  @volatile
  private var started = false

  def enqueue(taskItem: QueueItem.Task) = {
    requireStarted()

    // Get the max rebuild position and initialize it if necessary.
    def getOrInitializeRebuildEndPosition: Timestamp = synchronized {
      rebuildEndPositions.get(taskItem.priority) match {
        case Some(id) => id
        case None => {
          rebuildEndPositions += taskItem.priority -> taskItem.taskId
          taskItem.taskId
        }
      }
    }

    recorders.get(taskItem.priority) match {
      case Some(recorder) => {
        val request = item2request(taskItem)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))

        // Only update the task count if the enqueue task is after the max rebuild position.
        if (taskItem.taskId > getOrInitializeRebuildEndPosition) {
          totalTaskCount.incrementAndGet()
        }

      }
      case None => throw new IllegalArgumentException(s"Queue '$name' unknown priority ${taskItem.priority}")
    }
    taskItem
  }

  def ack(ackItem: QueueItem.Ack) = {
    requireStarted()

    feeder.pendingTaskPriorityFor(ackItem.taskId).flatMap(priority => recorders.get(priority)) match {
      case Some(recorder) => {
        val request = item2request(ackItem)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
        totalTaskCount.decrementAndGet()
      }
      case None => throw new IllegalArgumentException(s"Cannot acknowledge unknown task ${ackItem.taskId} for queue '$name'")
    }
    ackItem
  }

  val feeder = new LogQueueFeeder(definition, createPriorityTaskItemReader)

  def stats: QueueStats = LogQueueStats

  def getLastQueueItemId = recorders.valuesIterator.flatMap(_.currentConsistentTimestamp).reduceOption(Ordering[Timestamp].max)

  def readQueueItems(startItemId: Timestamp, endItemId: Timestamp) = {

    val sources = recorders.map {
      case (priority, recorder) => {
        val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
        val initialTimestamp = Ordering[Timestamp].max(findStartTimestamp(txLog), startItemId)
        val itr = new TransactionLogReplicationIterator(recorder.member,
          initialTimestamp, txLog, recorder.currentConsistentTimestamp)
        (priority, itr)
      }
    }.toMap

    new QueueItemReader(service, endItemId, sources)
  }

  def writeQueueItem(item: QueueItem) = item match {
    case taskItem: QueueItem.Task => enqueue(taskItem)
    case ackItem: QueueItem.Ack => ack(ackItem)
  }

  def truncateQueueItem(itemId: Timestamp) = {
    // TODO: Implement this method.
  }

  /**
   * Creates a new LogQueueFeeder.QueueReader. This method is passed as a factory function to the
   * LogQueueFeeder constructor.
   */
  private def createPriorityTaskItemReader(priority: Int, startTimestamp: Option[Timestamp]): PriorityTaskItemReader = {
    requireStarted()

    val recorder = recorders(priority)

    def createLogTaskReader: PriorityTaskItemReader = {
      val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
      val initialTimestamp = startTimestamp.getOrElse(findStartTimestamp(txLog))

      debug(s"Creating log queue reader '$name:$priority' starting at $initialTimestamp")

      // Rebuild in-memory states by reading all the persisted tasks from the transaction log once
      val (total, processed) = rebuildPriorityQueueState(priority, initialTimestamp)
      totalTaskCount.addAndGet(total)

      val itr = new TransactionLogReplicationIterator(recorder.member,
        initialTimestamp, txLog, recorder.currentConsistentTimestamp)
      PriorityTaskItemReader(service, itr, processed)
    }

    new DelayedPriorityTaskItemReader(recorder, createLogTaskReader)
  }

  /**
   * Reads all the persisted tasks from the transaction logs and computes the number of unprocessed tasks
   * (i.e. excluding acknowledged tasks) and keep a list of processed tasks (i.e. the acknowledged ones).
   * The processed tasks will be skip when read again from the feeder.
   */
  private def rebuildPriorityQueueState(priority: Int, initialTimestamp: Timestamp): (Int, Set[Timestamp]) = {

    import com.wajam.commons.Closable.using

    debug(s"Rebuilding queue state '$name:$priority'")

    val recorder = recorders(priority)
    val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
    val member = recorder.member
    val endTimestamp = synchronized(rebuildEndPositions(priority))

    using(new TransactionLogReplicationIterator(member, initialTimestamp, txLog, recorder.currentConsistentTimestamp)) { itr =>
      val items = for {
        msgOpt <- itr
        msg <- msgOpt
        item <- message2item(msg, service)
      } yield item

      // The log iterator does not return items beyond the consistent timestamp (i.e. `next` return None) and
      // `hasNext` continue to returns true. This behavior is fine for its original purpose i.e. stay open and
      // produce new transactions to replicate once they are appended to the log.
      // In our case we want to read tasks up to the `endTimestamp` inclusively.
      @tailrec
      def processNext(allItems: Set[Timestamp], processedItems: Set[Timestamp]): (Set[Timestamp], Set[Timestamp]) = {
        val (itemId, all, processed) = items.next() match {
          case taskItem: QueueItem.Task if taskItem.taskId <= endTimestamp => {
            (taskItem.taskId, allItems + taskItem.taskId, processedItems)
          }
          case taskItem: QueueItem.Task => (taskItem.taskId, allItems, processedItems)
          case ackItem: QueueItem.Ack if allItems.contains(ackItem.taskId) => {
            (ackItem.ackId, allItems - ackItem.taskId, processedItems + ackItem.taskId)
          }
          case ackItem: QueueItem.Ack => (ackItem.ackId, allItems, processedItems)
        }

        if (itemId != endTimestamp) processNext(all, processed) else (all, processed)
      }

      val (all, processed) = processNext(Set(), Set())

      debug(s"Rebuilding queue state '$name:$priority' completed (all=${all.size}, processed=${processed.size}})")

      (all.size, processed)
    }
  }

  /**
   * Returns the oldest log record timestamp starting the beginning of the transaction log. The records may be written
   * out of order in the log. So this methods reads more than the initial record returns the oldest timestamp.
   */
  private def findStartTimestamp(txLog: FileTransactionLog): Timestamp = {
    import com.wajam.commons.Closable.using

    debug(s"Finding the start timestamp '${txLog.service}'")

    using(txLog.read) { itr =>
      implicit val ord = Ordering[Option[Timestamp]]
      val firstTimestamp = txLog.firstRecord(timestamp = None).map(_.timestamp)
      val startRange = itr.takeWhile(_.consistentTimestamp < firstTimestamp).collect {
        case r: TimestampedRecord => r.timestamp.value
      }.min
      startRange
    }
  }

  def start() {
    if (!started) {
      debug(s"Starting queue '$name'")
      recorders.valuesIterator.foreach(_.start())
      started = true
    }
  }

  def stop() {
    if (started) {
      started = false
      debug(s"Stopping queue '$name'")
      recorders.valuesIterator.foreach(_.kill())
    }
  }

  private def requireStarted() {
    if (!started) {
      throw new IllegalStateException(s"Queue '$name' not started!")
    }
  }

  private object LogQueueStats extends QueueStats {
    def totalTasks = totalTaskCount.get()

    def pendingTasks = feeder.pendingTasks.toIterable

    def delayedTasks = feeder.delayedTasks.toIterable
  }

}

object LogQueue {

  object Logger extends Logging

  type RecorderFactory = (Long, QueueDefinition, Priority) => TransactionRecorder

  /**
   * Creates a new LogQueue. This factory method is usable as [[com.wajam.bwl.queue.Queue.QueueFactory]] with the
   * Bwl service
   */
  def create(dataDir: File, logFileRolloverSize: Int = 52428800, logCommitFrequency: Int = 2000)(token: Long, definition: QueueDefinition, service: Service)(implicit random: Random = Random): Queue = {

    Logger.debug(s"Create log queue '${definition.name}'")

    val logDir = Paths.get(dataDir.getCanonicalPath, service.name, "queues")
    Files.createDirectories(logDir)

    def consistencyDelay: Long = {
      val timestampTimeout = service.consistency match {
        case consistency: ConsistencyMasterSlave => consistency.timestampGenerator.responseTimeout
        case _ => 0
      }
      timestampTimeout + 250
    }

    def queueNameFor(priority: Priority) = s"${definition.name}#${priority.value}"

    def txLogFor(priority: Priority) = new FileTransactionLog(queueNameFor(priority), token, logDir.toString, logFileRolloverSize)

    def memberFor(priority: Priority) = {
      val ranges = ResolvedServiceMember(service, token).ranges
      ResolvedServiceMember(queueNameFor(priority), token, ranges) // TODO: Find a better solution to override service name
    }

    /**
     * Ensure the specified priority log tail is properly ended by an Index record.
     */
    def finalizeLog(priority: Priority) {
      val txLog = txLogFor(priority)
      txLog.getLastLoggedRecord match {
        case Some(record: TimestampedRecord) => {
          // Not ended by an Index record. We need to rewrite the log tail to find the last record timestamp.
          val recovery = new ConsistencyRecovery(txLog.logDir, DummyTruncatableConsistentStore)
          recovery.rewriteFromRecord(record, txLog, memberFor(priority))
        }
        case Some(_: Index) => // Nothing to do, the log is ended by an Index record!!!
        case None => // Nothing to do, the log is empty.
      }
    }

    // Ensure that all logs are properly finalized with an Index record
    definition.priorities.foreach(finalizeLog)

    val recorderFactory: RecorderFactory = (token, definition, priority) => {
      Logger.debug(s"Create recorder '${queueNameFor(priority)}'")
      new TransactionRecorder(memberFor(priority), txLogFor(priority), consistencyDelay, service.responseTimeout, logCommitFrequency, {})
    }

    new LogQueue(token, service, definition, recorderFactory)
  }

  private[log] def message2item(message: Message, service: Service): Option[QueueItem] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._

    message.function match {
      case MessageType.FUNCTION_CALL if message.path == "/enqueue" => {
        message.timestamp.map(QueueItem.Task(_, message.token, message.param[Int](TaskPriority), message.getData[Any]))
      }
      case MessageType.FUNCTION_CALL if message.path == "/ack" => message.timestamp.map(QueueItem.Ack(_, message.param[Long](TaskId)))
      case _ => throw new IllegalStateException(s"Unsupported message path: ${message.path}")
    }
  }

  private[log] def item2request(item: QueueItem): InMessage = {
    item match {
      case taskItem: QueueItem.Task => {
        val params = Iterable[(String, MValue)](TaskPriority -> taskItem.priority)
        createSyntheticRequest(taskItem.taskId, taskItem.token, "/enqueue", params, taskItem.data)
      }
      case ackItem: QueueItem.Ack => createSyntheticRequest(ackItem.ackId, -1, "/ack")
    }
  }

  private[log] def createSyntheticRequest(taskId: Timestamp, taskToken: Long, path: String,
                                          params: Iterable[(String, MValue)] = Nil, data: Any = null): InMessage = {
    val extraParams = Iterable[(String, MValue)](TaskId -> taskId.value, TaskToken -> taskToken)
    val request = new InMessage(params ++ extraParams, data = data)
    request.token = taskToken
    request.timestamp = Some(taskId)
    request.function = MessageType.FUNCTION_CALL
    request.path = path
    request
  }

  private[log] def createSyntheticSuccessResponse(request: InMessage): OutMessage = {
    val response = new OutMessage(code = 200)
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response
  }

  /**
   * Dummy ConsistentStore which implement a no-op `truncateAt` method. Calling any other method raise an error.
   * Invoked if a transaction is incomplete in the logs (i.e. request is written but response is missing) when
   * rewriting tail of the queue transaction log. Since the transaction log ARE the storage, there is nothing to
   * truncate in the underlying non-existent store. The other methods are not called when rewriting the log tail and
   * failing ensure awareness if they are in the future.
   */
  private[log] object DummyTruncatableConsistentStore extends ConsistentStore {
    def requiresConsistency(message: Message) = ???
    def getLastTimestamp(ranges: Seq[TokenRange]) = ???
    def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp) = ???
    def readTransactions(fromTime: Timestamp, toTime: Timestamp, ranges: Seq[TokenRange]) = ???
    def writeTransaction(message: Message) = ???
    def truncateAt(timestamp: Timestamp, token: Long) = {}
  }
}
