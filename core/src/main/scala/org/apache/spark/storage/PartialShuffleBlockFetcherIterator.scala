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

package org.apache.spark.storage

import java.io.InputStream

import org.apache.spark.MapOutputTracker._
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.{Logging, MapOutputTracker, SparkEnv, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * A shuffle block fetcher that works even when the map output is not completed.
  * This is done by wrapping around the ShuffleBlockFetcher and refresh it as needed
  */
class PartialShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    shuffleId: Int,
    startPartition: Int,
    endPartition: Int,
    maxBytesInFlight: Long)
  extends Iterator[(BlockId, InputStream)] with Logging{

  private val cacheManager = SparkEnv.get.cacheManager

  private[this] val shuffleMetrics = context.taskMetrics().createShuffleReadMetricsForDependency()

  private val initTime = System.currentTimeMillis()
  private var firstRead = true

  private val readyBlocks = new mutable.HashSet[Int]()

  private var blockFetcherIter: ShuffleBlockFetcherIterator = null

  private var finished: Boolean = false

  private var fetchEpoch = 0

  private var reOffered = false

  private val waiter = context.partialWaiter()

  waiter.register()
  // Initialize the block fetcher
  refreshBlockFetcher()

  override def hasNext: Boolean = {
    if (finished) {
      blockFetcherIter.hasNext
    } else {
      if (!blockFetcherIter.hasNext) {
        if (!finished && !reOffered) {
          // If the map output is still incomplete & we haven't paused yet & we don't hold any lock on RDD
          // We want task that keep lock on RDD to run as fast as possible, so we never pause those
          reOffered = true
          log.info("Task {} has incomplete map output. Try to run other tasks", context.taskAttemptId())
          if (cacheManager.hasLock(context.taskAttemptId())) {
            log.info("Not pausing task {} because it's holding lock", context.taskAttemptId())
            context.executorBackend().reOffer(context.taskAttemptId(), shared = true)
          } else {
            log.info("Pausing task {}", context.taskAttemptId())
            context.executorBackend().reOffer(context.taskAttemptId(), shared = false)
            waiter.arriveAndAwaitAdvance()
            log.info("Task {} resumed", context.taskAttemptId())
          }
        }
        refreshBlockFetcher()
      }
      blockFetcherIter.hasNext
    }
  }

  override def next(): (BlockId, InputStream) = {
    blockFetcherIter.next()
  }

  /**
    * Refresh the block fetcher. Block until we have new block or there is nothing to read
    */
  private def refreshBlockFetcher(): Unit = {
    val startWaitTime = System.currentTimeMillis()
    // This will block until we have something new to return
    log.info("Task {}, old epoch: {}", context.taskAttemptId(), fetchEpoch)
    val out = mapOutputTracker.getUpdatedStatus(shuffleId, fetchEpoch)
    fetchEpoch = out._2
    log.info("Task {}, new epoch: {}", context.taskAttemptId(), fetchEpoch)
    val statuses = out._1.synchronized {
      Array[MapStatus]() ++ out._1
    }
    var statusWithIndex = statuses.zipWithIndex

    statusWithIndex = statuses.zipWithIndex
    if (firstRead) {
      firstRead = false
      shuffleMetrics.incInitialReadTime(System.currentTimeMillis() - initTime)
    } else {
      // Calculate the time that we wait
      val duration = System.currentTimeMillis() - startWaitTime
      log.info("Waiting {} ms for new block to be available", duration)
      shuffleMetrics.incWaitForPartialOutputTime(duration)
      // Store the period that tasks wait for parent's output
      shuffleMetrics.addWaitForParentPeriod((startWaitTime, duration))
    }

    val splitsByAddress = new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    statusWithIndex.foreach { case (status, mapId) =>
      if (status != null && !readyBlocks.contains(mapId)) {
        for (part <- startPartition until endPartition) {
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
        readyBlocks += mapId
      }
    }
    log.info("Time waiting for partial output: {}", shuffleMetrics.waitForPartialOutputTime)
    log.info("Task {} has {} blocks ready", context.taskAttemptId(), readyBlocks.size)

    if (readyBlocks.size == statuses.length) {
      finished = true
      waiter.arriveAndDeregister()
    }

    blockFetcherIter = new ShuffleBlockFetcherIterator(
      context,
      shuffleClient,
      blockManager,
      splitsByAddress.toSeq,
      maxBytesInFlight)
  }
}
