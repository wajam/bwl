package com.wajam.bwl.utils

import com.wajam.nrv.utils.Closable

/**
 * Iterator decorator which allows peeking at the next element. This implementation read ahead the next element.
 */
class PeekIterator[T](itr: Iterator[T]) extends Iterator[T] {

  private var nextElem: Option[T] = getNextElem()

  def peek: T = nextElem.get

  def hasNext = {
    nextElem.isDefined
  }

  def next() = {
    val value = nextElem
    nextElem = getNextElem()
    value.get
  }

  private def getNextElem(): Option[T] = {
    if (itr.hasNext) {
      Some(itr.next())
    } else None
  }
}

object PeekIterator {
  def apply[T](itr: Iterator[T]): PeekIterator[T] = new PeekIterator(itr)
}

class ClosablePeekIterator[T](itr: Iterator[T] with Closable) extends PeekIterator[T](itr) with Closable {
  def close() = itr.close()
}