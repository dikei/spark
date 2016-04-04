/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.ExternalAppendOnlyMap

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    timeout: Option[Int] = None,
    interval: Option[Int] = None) {

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)

  def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    interval match {
      case None =>
        val combiners = new ExternalAppendOnlyMap[K, V, C](
          createCombiner, mergeValue, mergeCombiners
        )
        combiners.insertAll(iter, timeout)
        updateMetrics(context, combiners)
        combiners.iterator
      case Some(time) =>
        new AggregateValueByIntervalInterator[K, V, C](
          iter, context, createCombiner, mergeValue, mergeCombiners, time
        )
    }
  }

  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]) : Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

  def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    interval match {
      case None =>
        val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
        combiners.insertAll(iter, timeout)
        updateMetrics(context, combiners)
        combiners.iterator
      case Some(time) =>
        new AggregateCombinerByIntervalIterator[K, C](iter, context, mergeCombiners, time)
    }
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
    }
  }
}

private class AggregateValueByIntervalInterator[K, V, C](
    iter: Iterator[_ <: Product2[K, V]],
    context: TaskContext,
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    interval: Int) extends Iterator[(K, C)] with Logging {

  var accumulator = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
  var currentIter: Iterator[(K, C)] = null

  override def hasNext: Boolean = {
    iter.hasNext || (currentIter != null && currentIter.hasNext)
  }

  override def next(): (K, C) = {
    // Load the map for the next interval
    if (iter.hasNext && (currentIter == null || !currentIter.hasNext)) {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter, Some(interval))
      updateMetrics(context, combiners)

      // Merge into accumulator before returning
      accumulator.insertAll(combiners.iterator)
      currentIter = accumulator.iterator

      // Create new accumulator if we can still read from the iterator
      if (iter.hasNext) {
        accumulator = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      }
    }
    val (retKey, retCom) = currentIter.next

    // Store the accumulated value if needed
    if (iter.hasNext) {
      accumulator.insert(retKey, retCom)
    }

    (retKey, retCom)
  }

  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
    }
  }
}

private class AggregateCombinerByIntervalIterator[K, C](
   iter: Iterator[_ <: Product2[K, C]],
   context: TaskContext,
   mergeCombiners: (C, C) => C,
   interval: Int) extends Iterator[(K, C)] with Logging {

  var accumulator = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
  var currentIter: Iterator[(K, C)] = null

  override def hasNext: Boolean = {
    iter.hasNext || (currentIter != null && currentIter.hasNext)
  }

  override def next(): (K, C) = {
    // Load the map for the next interval
    if (iter.hasNext && (currentIter == null || !currentIter.hasNext)) {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      combiners.insertAll(iter, timeout = Some(interval))
      updateMetrics(context, combiners)
      // Merge with accumulator
      accumulator.insertAll(combiners.iterator)
      currentIter = accumulator.iterator

      // Create new accumulator if we can still read from the iterator
      if (iter.hasNext) {
        accumulator = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      }
    }
    val (retKey, retCom) = currentIter.next

    // Store the accumulated value if needed
    if (iter.hasNext) {
      accumulator.insert(retKey, retCom)
    }

    (retKey, retCom)
  }

  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
    }
  }
}
