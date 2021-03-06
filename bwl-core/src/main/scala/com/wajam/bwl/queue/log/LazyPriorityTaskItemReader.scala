package com.wajam.bwl.queue.log

import com.wajam.nrv.consistency.TransactionRecorder
import com.wajam.bwl.queue.log.LazyPriorityTaskItemReader.InfiniteEmptyPriorityTaskItemReader

/**
 * Queue reader wrapper which ensure that log exists and is NOT empty before creating the real PriorityTaskItemReader.
 * Achieved by waiting until TransactionRecorder produces a valid consistent timestamp.
 */
class LazyPriorityTaskItemReader(recorder: TransactionRecorder, createReader: => PriorityTaskItemReader)
    extends PriorityTaskItemReader {

  private var reader: Option[PriorityTaskItemReader] = None

  private def getOrCreateReader: PriorityTaskItemReader = reader match {
    case None if recorder.currentConsistentTimestamp.isEmpty => InfiniteEmptyPriorityTaskItemReader
    case None => {
      val itr = createReader
      reader = Some(itr)
      itr
    }
    case Some(itr) => itr
  }

  def hasNext = getOrCreateReader.hasNext

  def next() = getOrCreateReader.next()

  def close() {
    reader.foreach(_.close())
  }

}

object LazyPriorityTaskItemReader {
  private object InfiniteEmptyPriorityTaskItemReader extends PriorityTaskItemReader {
    def hasNext = true

    def next() = None

    def close() {}

  }
}

