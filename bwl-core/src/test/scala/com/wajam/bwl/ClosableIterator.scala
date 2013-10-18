package com.wajam.bwl

import com.wajam.commons.Closable
import language.implicitConversions

object ClosableIterator {
  /**
   * Implicitly convert an Iterator to a closable iterator. Use in tests where we don't really care about the
   * close functionality.
   */
  implicit def toClosable[T](itr: Iterator[T]): Iterator[T] with Closable = {
    new Iterator[T] with Closable {
      def hasNext = itr.hasNext

      def next() = itr.next()

      def close() = {}
    }
  }
}
