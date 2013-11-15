package com.wajam.bwl.queue.log

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.data.MValue._
import java.io.File
import java.nio.file.{ Files, Paths }
import com.wajam.nrv.consistency._
import com.wajam.nrv.service.{ TokenRange, Service }
import com.wajam.nrv.consistency.log.{ LogRecord, TimestampedRecord, FileTransactionLog }
import com.wajam.nrv.consistency.replication.{ ReplicationSourceIterator, TransactionLogReplicationIterator }
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }
import scala.util.Random
import scala.annotation.tailrec
import com.wajam.nrv.consistency.log.LogRecord.Index
import com.wajam.commons.{ Closable, Logging }
import LogQueue._
import com.wajam.bwl.utils.PeekIterator
import scala.concurrent.ExecutionContext
import java.util.concurrent.{ TimeUnit, Executors }

/**
 * Persistent queue using NRV transaction log as backing storage. Each priority is appended to separate log.
 */
class LogQueue(val token: Long, val definition: QueueDefinition, recorderFactory: RecorderFactory,
               logCleanFrequencyInMs: Long, truncateTracker: TruncateTracker)(implicit random: Random = Random)
    extends ConsistentQueue with Logging {

  private val recorders: Map[Int, TransactionRecorder] = priorities.map(p => p.value -> recorderFactory(token, definition, p)).toMap

  // Must not read tasks from logs beyond this position when rebuilding the initial state (rebuild is lazy and per priority),
  // By default the max rebuild position is the last item present in the log at the time the queue is created.
  // The state is also updated as new task are enqueued. The max rebuild position ensure that tasks enqueued before the
  // state is rebuilt are not computed twice. If the queue is empty and has no log, the max rebuild position is
  // initialized later at the first enqueued task.
  private var rebuildEndPositions: Map[Int, Timestamp] = recorders.collect {
    case (priority, recorder) if recorder.currentConsistentTimestamp.isDefined => priority -> recorder.currentConsistentTimestamp.get
  }.toMap

  private val cleanupExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  private val cleaners: Map[Int, LogCleaner] = recorders.map {
    case (priority, recorder) => {
      implicit val ec = cleanupExecutionContext
      priority -> new LogCleaner(recorder.txLog.asInstanceOf[FileTransactionLog], oldestItemIdFor(priority), logCleanFrequencyInMs)
    }
  }

  private val lastItemId = new AtomicTimestamp(AtomicTimestamp.updateIfGreater,
    recorders.valuesIterator.flatMap(_.currentConsistentTimestamp).reduceOption(Ordering[Timestamp].max))

  private var openReadIterators: List[PeekIterator[QueueItem]] = Nil

  private val totalTaskCount = new AtomicInteger()

  private var started = new AtomicBoolean(false)

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
        lastItemId.update(Some(taskItem.itemId))

        // Only update the task count if the enqueue task is after the max rebuild position.
        if (taskItem.taskId > getOrInitializeRebuildEndPosition) {
          totalTaskCount.incrementAndGet()
        }

      }
      case None => throw new IllegalArgumentException(s"Queue '$token:$name' unknown priority ${taskItem.priority}")
    }
    taskItem
  }

  def ack(ackItem: QueueItem.Ack) = {
    requireStarted()

    recorders.get(ackItem.priority) match {
      case Some(recorder) => {
        val request = item2request(ackItem)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
        lastItemId.update(Some(ackItem.itemId))

        if (feeder.isPending(ackItem.taskId)) {
          totalTaskCount.decrementAndGet()
        }

        cleaners(ackItem.priority).tick()
      }
      case None => throw new IllegalArgumentException(s"Queue '$token:$name' unknown priority ${ackItem.priority}")
    }
    ackItem
  }

  val feeder = new LogQueueFeeder(definition, createPriorityTaskItemReader)

  def stats: QueueStats = LogQueueStats

  def getLastQueueItemId = {
    lastItemId.get match {
      case Some(lastId) if truncateTracker.contains(lastId) => {
        debug(s"Queue '$token:$name' last item '$lastId' is truncated. Fallback to a slower method to find non-truncated id.")
        val ids: Iterator[Timestamp] = recorders.valuesIterator.flatMap(recorder =>
          getLastNonTruncatedQueueItemId(recorder.txLog.asInstanceOf[FileTransactionLog]))
        ids.reduceOption(Ordering[Timestamp].max)
      }
      case lastIdOpt => lastIdOpt
    }
  }

  def readQueueItems(startItemId: Timestamp, endItemId: Timestamp) = {

    val sources = recorders.map {
      case (priority, recorder) => {
        val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
        val itr = if (txLog.getLogFiles.isEmpty) {
          // Empty log
          new ReplicationSourceIterator {
            val start = startItemId
            val end = Some(endItemId)

            def hasNext = false

            def next() = throw new NotImplementedError

            def close() = {}
          }
        } else {
          // Filter out truncated items
          createPriorityLogSourceIterator(priority, Some(startItemId), recorder.currentConsistentTimestamp).withFilter {
            case Some(msg) => !truncateTracker.contains(msg.timestamp.get)
            case None => true
          }
        }

        (priority, itr)
      }
    }.toMap

    val reader = new QueueItemReader(endItemId, sources)
    new PeekIterator(reader) with Closable {
      // Keep track of the open read iterators to ensure the cleanup job is not deleted files while they are read.
      // Use PeekIterator so the cleanup job knows the next queue item read without consuming it.
      LogQueue.this.synchronized(openReadIterators = this :: openReadIterators)

      def close() = {
        LogQueue.this.synchronized(openReadIterators = openReadIterators.filterNot(_ == this))
        reader.close()
      }
    }
  }

  def writeQueueItem(item: QueueItem) = item match {
    case taskItem: QueueItem.Task => enqueue(taskItem)
    case ackItem: QueueItem.Ack => ack(ackItem)
  }

  def truncateQueueItem(itemId: Timestamp) = {
    truncateTracker.truncate(itemId)
  }

  private def createPriorityLogSourceIterator(priority: Int, startTimestamp: Option[Timestamp],
                                              currentConsistentTimestamp: => Option[Timestamp],
                                              isDraining: => Boolean = false): ReplicationSourceIterator = {
    val recorder = recorders(priority)
    val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
    val member = recorder.member

    val firstTimestamp = findStartTimestamp(txLog)
    debug(s"First '$token:$name#$priority' log timestamp is $firstTimestamp")

    val (initialTimestamp, minTimestamp) = startTimestamp match {
      case Some(start) => {
        // The startTimestamp may or may not exist in the priority log. The following code ensure that we start
        // reading from an existing timestamp prior startTimestamp.
        val initialTimestamp = txLog.guessLogFile(start).map(file => txLog.getIndexFromName(file.getName)) match {
          case Some(Index(_, Some(consistentTimestamp))) => Ordering[Timestamp].max(consistentTimestamp, firstTimestamp)
          case _ => firstTimestamp
        }
        (initialTimestamp, start)
      }
      case None => (firstTimestamp, firstTimestamp)
    }

    debug(s"Read '$token:$name#$priority' log from $initialTimestamp but filtering items before $minTimestamp")

    // Filter out items prior minTimestamp
    new TransactionLogReplicationIterator(member, initialTimestamp, txLog, currentConsistentTimestamp, isDraining).withFilter {
      case Some(msg) => msg.timestamp.get >= minTimestamp
      case None => true
    }
  }

  /**
   * Creates a new LogQueueFeeder.QueueReader. This method is passed as a factory function to the
   * LogQueueFeeder constructor.
   */
  private def createPriorityTaskItemReader(priority: Int, startTimestamp: Option[Timestamp]): PriorityTaskItemReader = {
    requireStarted()

    val recorder = recorders(priority)

    def createLogTaskReader: PriorityTaskItemReader = {
      debug(s"Creating log queue reader '$token:$name#$priority' starting at $startTimestamp")

      // Rebuild in-memory states by reading all the persisted tasks from the transaction log once
      val (total, processed) = rebuildPriorityQueueState(priority, startTimestamp)
      totalTaskCount.addAndGet(total)

      val itr = createPriorityLogSourceIterator(priority, startTimestamp, recorder.currentConsistentTimestamp).withFilter {
        case Some(msg) => !truncateTracker.contains(msg.timestamp.get)
        case None => true
      }
      PriorityTaskItemReader(itr, processed)
    }

    new DelayedPriorityTaskItemReader(recorder, createLogTaskReader)
  }

  /**
   * Reads all the persisted tasks from the transaction logs and computes the number of unprocessed tasks
   * (i.e. excluding acknowledged tasks) and keep a list of processed tasks (i.e. the acknowledged ones).
   * The processed tasks will be skip when read again from the feeder.
   */
  private def rebuildPriorityQueueState(priority: Int, startTimestamp: Option[Timestamp]): (Int, Set[Timestamp]) = {

    import com.wajam.commons.Closable.using

    debug(s"Rebuilding queue state '$token:$name#$priority' starting at $startTimestamp")

    val recorder = recorders(priority)
    val endTimestamp = synchronized(rebuildEndPositions(priority))

    using(createPriorityLogSourceIterator(priority, startTimestamp, Some(Long.MaxValue), isDraining = true)) { itr =>
      // The log replication source does not return items beyond the consistent timestamp (i.e. `next` return None) and
      // `hasNext` continue to returns true. This behavior is fine for its original purpose i.e. stay open and
      // produce new transactions to replicate once they are appended to the log.
      // In our case we want to read tasks up to the `endTimestamp` inclusively.
      @tailrec
      def processNext(allItems: Set[Timestamp], processedItems: Set[Timestamp]): (Set[Timestamp], Set[Timestamp]) = {
        requireStarted()

        if (itr.hasNext) itr.next() match {
          case Some(msg) => {
            val (itemId, all, processed) = message2item(msg) match {
              case Some(taskItem: QueueItem.Task) if taskItem.taskId <= endTimestamp &&
                !truncateTracker.contains(taskItem.taskId) => {
                (taskItem.taskId, allItems + taskItem.taskId, processedItems)
              }
              case Some(ackItem: QueueItem.Ack) if allItems.contains(ackItem.taskId) &&
                !truncateTracker.contains(ackItem.ackId) => {
                (ackItem.ackId, allItems - ackItem.taskId, processedItems + ackItem.taskId)
              }
              case Some(item) => (item.itemId, allItems, processedItems)
            }

            if (itemId < endTimestamp) processNext(all, processed) else (all, processed)
          }
          case None => processNext(allItems, processedItems)
        }
        else {
          // Source is exhausted, stop here!
          (allItems, processedItems)
        }
      }

      val (all, processed) = processNext(Set(), Set())

      debug(s"Rebuilding queue state '$token:$name#$priority' completed (all=${all.size}, processed=${processed.size}})")

      (all.size, processed)
    }
  }

  /**
   * Returns the oldest log record timestamp starting the beginning of the transaction log. The records may be written
   * out of order in the log. So this methods reads more than the initial record returns the oldest timestamp.
   */
  private def findStartTimestamp(txLog: FileTransactionLog): Timestamp = {
    import com.wajam.commons.Closable.using

    debug(s"Finding the start timestamp '$token:${txLog.service}'")

    using(txLog.read) { itr =>
      implicit val ord = Ordering[Option[Timestamp]]

      val firstRecordTimestamp = txLog.firstRecord(timestamp = None).map(_.timestamp)

      val cleaned = txLog.getIndexFromName(txLog.getLogFiles.head.getName).consistentTimestamp.nonEmpty
      val (initialRangeFirstTimestamp: Option[Timestamp], initialRange: Iterator[LogRecord]) = if (cleaned) {
        // Log files has been cleaned at least once i.e. the first record consistent timestamp refer to a record
        // written in a now deleted log file. We skip all records with a consistent timestamp smaller than the first
        // record timestamp to ensure that the computed `startTimestamp` consistent timestamp exist. Necessary
        // or `TransactionLogReplicationIterator` will fail because it tries to read starting at the
        // `startTimestamp` consistent timestamp .
        val initialRange: PeekIterator[TimestampedRecord] =
          PeekIterator(itr.dropWhile(_.consistentTimestamp < firstRecordTimestamp).collect { case r: TimestampedRecord => r })
        val initialRangeFirstTimestamp = if (initialRange.hasNext) Some(initialRange.peek.timestamp) else None
        (initialRangeFirstTimestamp, initialRange)
      } else {
        (firstRecordTimestamp, itr)
      }

      val initialTimestamp = initialRange.takeWhile(_.consistentTimestamp < initialRangeFirstTimestamp).collect {
        case r: TimestampedRecord => r.timestamp.value
      }.min
      initialTimestamp
    }
  }

  def start() {
    if (started.compareAndSet(false, true)) {
      debug(s"Starting queue '$token:$name'")
      recorders.valuesIterator.foreach(_.start())

      // The only case we truncate is after the recovery process (on service member up when we check the log and the
      // store (the queue) timestamps and possibly truncate record that are in the LogQueue but not in the transaction
      // logs. So if there are no replication, the tracker will always be empty. Also, this means that the tracker
      // timestamp set should be fairly small at all time (which means we do not need to worry about keeping that
      // in memory and compacting only on restart).
      truncateTracker.compact(oldestLogConsistentTimestamp)
    }
  }

  def stop() {
    if (started.compareAndSet(true, false)) {
      debug(s"Stopping queue '$token:$name'")
      try {
        cleanupExecutionContext.shutdown()
        cleanupExecutionContext.awaitTermination(2000L, TimeUnit.MILLISECONDS)
      } catch {
        case e: InterruptedException => warn(s"Timeout while shutting down queue '$token:$name' executor service.")
      }
      recorders.valuesIterator.foreach(_.kill())
    }
  }

  private def requireStarted() {
    if (!started.get()) {
      throw new IllegalStateException(s"Queue '$token:$name' not started!")
    }
  }

  private object LogQueueStats extends QueueStats {
    def totalTasks = totalTaskCount.get()

    def pendingTasks = feeder.pendingTasks.toIterable

    def delayedTasks = feeder.delayedTasks.toIterable
  }

  private def getLastNonTruncatedQueueItemId(txLog: FileTransactionLog): Option[Timestamp] = {
    import com.wajam.commons.Closable.using

    def maxNonTruncatedQueueItemStartingAt(file: File): Option[Timestamp] = {
      val fileIndex = txLog.getIndexFromName(file.getName)
      using(txLog.read(fileIndex).toSafeIterator) { itr =>
        itr.collect {
          case record: TimestampedRecord if !truncateTracker.contains(record.timestamp) => record.timestamp
        }.reduceOption(Ordering[Timestamp].max)
      }
    }

    // Iterate log files in reverse order to find last non-truncated record. Stop at the first log file which
    // contains an un-truncated record.
    txLog.getLogFiles.toList.reverse.toIterator.flatMap(file =>
      maxNonTruncatedQueueItemStartingAt(file)).collectFirst { case timestamp => timestamp }
  }

  /**
   * Find the oldest item between the feeder task processed and the items currently being read for replication
   */
  private[log] def oldestItemIdFor(priority: Int): Option[Timestamp] = {
    val processed: Option[Timestamp] = feeder.oldestTaskIdFor(priority)
    val replicated: Option[Timestamp] = synchronized {
      openReadIterators.collect { case itr if itr.hasNext => itr.peek.itemId }.reduceOption(Ordering[Timestamp].min)
    }
    // Returns the smallest of the non-empty values
    (processed ++ replicated).reduceOption(Ordering[Timestamp].min)
  }

  private[log] def oldestLogConsistentTimestamp: Option[Timestamp] = {

    def getIndexesFromLogFileNames(txLog: FileTransactionLog): Iterable[Index] = {
      txLog.getLogFiles.map(file => txLog.getIndexFromName(file.getName))
    }

    // Find the oldest known consistent timestamp for each priority by using consistent timestamp from log file names
    val prioritiesFirstConsistentTimestamps = {
      for {
        recorder <- recorders.valuesIterator
        txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
        index <- getIndexesFromLogFileNames(txLog)
        consistentTimestamp <- index.consistentTimestamp
      } yield consistentTimestamp
    }.toList

    // Returns the smallest value
    prioritiesFirstConsistentTimestamps.reduceOption(Ordering[Timestamp].min)
  }
}

object LogQueue {

  object Logger extends Logging

  type RecorderFactory = (Long, QueueDefinition, Priority) => TransactionRecorder

  class Factory(dataDir: File, logFileRolloverSize: Int = 52428800, logCommitFrequency: Int = 2000,
                logCleanFrequencyInMs: Long = 3600000L)(implicit random: Random = Random) extends QueueFactory {
    def createQueue(token: Long, definition: QueueDefinition, service: Service) = {
      Logger.debug(s"Create log queue '$token:${definition.name}'")

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
        ResolvedServiceMember(queueNameFor(priority), token, ranges)
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

      val truncateTracker = new TruncateTracker(new File(logDir.toFile, s"${definition.name}.truncate"))

      val recorderFactory: RecorderFactory = (token, definition, priority) => {
        Logger.debug(s"Create recorder '$token:${queueNameFor(priority)}'")
        new TransactionRecorder(memberFor(priority), txLogFor(priority), consistencyDelay, service.responseTimeout, logCommitFrequency, {})
      }

      new LogQueue(token, definition, recorderFactory, logCleanFrequencyInMs, truncateTracker)
    }
  }

  private[log] def message2item(message: Message): Option[QueueItem] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._

    message.function match {
      case MessageType.FUNCTION_CALL if message.path == "/enqueue" => {
        message.timestamp.map(QueueItem.Task(message.param[String](QueueName), message.token,
          message.param[Int](TaskPriority), _, message.getData[Any]))
      }
      case MessageType.FUNCTION_CALL if message.path == "/ack" => {
        message.timestamp.map(QueueItem.Ack(message.param[String](QueueName), message.token,
          message.param[Int](TaskPriority), _, taskId = message.param[Long](TaskId)))
      }
      case _ => throw new IllegalStateException(s"Unsupported message path: ${message.path}")
    }
  }

  private[log] def item2request(item: QueueItem): InMessage = {
    item match {
      case taskItem: QueueItem.Task => {
        createSyntheticRequest(taskItem.taskId, taskItem.taskId, taskItem, "/enqueue", taskItem.data)
      }
      case ackItem: QueueItem.Ack => createSyntheticRequest(ackItem.ackId, ackItem.taskId, ackItem, "/ack")
    }
  }

  private[log] def createSyntheticRequest(itemId: Timestamp, taskId: Timestamp, item: QueueItem, path: String,
                                          data: Any = null): InMessage = {
    val params = Iterable[(String, MValue)](QueueName -> item.name, TaskId -> taskId.value, TaskToken -> item.token,
      TaskPriority -> item.priority)
    val request = new InMessage(params, data = data)
    request.token = item.token
    request.timestamp = Some(itemId)
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
