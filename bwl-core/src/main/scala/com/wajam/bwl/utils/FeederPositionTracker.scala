package com.wajam.bwl.utils

import scala.collection.immutable.TreeSet

/**
 * Helper class to track the oldest pending/processed `Feeder` item position held in `TaskContext`.
 * It is up to the `Feeder` to read and store the position in its `TaskContext`.
 * This class only helps to compute the oldest position the `Feeder` must resume from.
 * It is assumed that the feeder is idempotent and that items starting at the `oldestItemId` inclusively will be
 * processed when the feeder resume from the position stored in `TaskContext`.
 */
class FeederPositionTracker[T](initialItemId: Option[T])(implicit ord: Ordering[T]) {
  private var pendingItems: Set[T] = TreeSet()
  private var lastItemId: Option[T] = initialItemId

  /**
   * Add an item to track when the feeder returns a new item.
   */
  def +=(itemId: T) {
    pendingItems += itemId

    // Keep the most recent item id
    lastItemId = lastItemId match {
      case Some(last) => Some(ord.max(itemId, last))
      case None => Some(itemId)
    }
  }

  /**
   * Remove a pending item when it is acknowledged.
   */
  def -=(itemId: T) {
    pendingItems -= itemId
  }

  /**
   * Returns the current oldest item id that should be stored in the `Feeder`'s `TaskContext`
   */
  def oldestItemId: Option[T] = {
    pendingItems.headOption match {
      case head @ Some(_) => head
      case None => lastItemId
    }
  }

  def contains(itemId: T): Boolean = pendingItems.contains(itemId)
}

