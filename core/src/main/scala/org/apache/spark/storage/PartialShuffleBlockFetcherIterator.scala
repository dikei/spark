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

  private val minRefreshInterval =
    SparkEnv.get.conf.getLong("spark.shuffle.minMapOutputRefreshInterval", 1000)
  private val maxRefreshInterval =
    SparkEnv.get.conf.getLong("spark.shuffle.maxMapOutputRefreshInterval", 4000)

  private[this] val shuffleMetrics = context.taskMetrics().createShuffleReadMetricsForDependency()

  private val initTime = System.currentTimeMillis()
  private var firstRead = true

  private val readyBlocks = new mutable.HashSet[Int]()

  private var blockFetcherIter: ShuffleBlockFetcherIterator = null

  private var finished: Boolean = false

  // Initialize the block fetcher
  refreshBlockFetcher()

  override def hasNext: Boolean = {
    if (finished) {
      blockFetcherIter.hasNext
    } else {
      if (!blockFetcherIter.hasNext) {
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
    def statuses: Array[MapStatus] = {
      val out = mapOutputTracker.getStatuses(shuffleId)
      // Copy the array map status array
      out.synchronized {
        Array[MapStatus]() ++ out
      }
    }

    var statusWithIndex = statuses.zipWithIndex
    var newBlocksAvailable = statusWithIndex.exists {
      case (s, i) => s != null && !readyBlocks.contains(i)
    }
    var refreshInterval = minRefreshInterval
    while (!newBlocksAvailable) {
      if (firstRead) {
        firstRead = false
        shuffleMetrics.incInitialReadTime(System.currentTimeMillis() - initTime)
      }
      val startWaitTime = System.currentTimeMillis()
      // Wait until new block is available
      log.info("Waiting {} ms for new block to be available", refreshInterval)
      Thread.sleep(refreshInterval)
      statusWithIndex = statuses.zipWithIndex
      newBlocksAvailable = statusWithIndex.exists {
        case (s, i) => s != null && !readyBlocks.contains(i)
      }
      shuffleMetrics.incWaitForPartialOutputTime(System.currentTimeMillis() - startWaitTime)
      // Increase the refresh interval for the next loop
      refreshInterval = Math.min(refreshInterval * 2, maxRefreshInterval)
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

    if (readyBlocks.size == statuses.length) {
      finished = true
    }

    blockFetcherIter = new ShuffleBlockFetcherIterator(
      context,
      shuffleClient,
      blockManager,
      splitsByAddress.toSeq,
      maxBytesInFlight)
  }
}
