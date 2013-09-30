package com.wajam.bwl.utils

import scala.util.Random

/**
 * Select next item T randomly based on each item weight.
 * Scala adaptation of http://stackoverflow.com/questions/6409652/random-weighted-selection-java-framework
 */
class WeightedItemsSelector[T](weightedItems: Iterable[(Double, T)]) {

  private val (distributedMap, total) = weightedItems.foldLeft((new java.util.TreeMap[Double, T], 0.0))((acc, entry) => {
    val (map, tot) = acc
    val (weight, item) = entry
    map.put(tot + weight, item)
    (map, tot + weight)
  })

  /**
   * Select next item T randomly
   */
  def next: T = distributedMap.ceilingEntry(Random.nextDouble() * total).getValue
}