package com.wajam.bwl

import com.wajam.commons.Closable
import language.implicitConversions

object ClosableIterator {
  implicit def toClosable[T](itr: Iterator[T]): Iterator[T] with Closable = {
    new Iterator[T] with Closable {
      def hasNext = itr.hasNext

      def next() = itr.next()

      def close() = {}
    }
  }
}
