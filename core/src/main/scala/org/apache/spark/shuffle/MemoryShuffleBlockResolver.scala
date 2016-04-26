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

package org.apache.spark.shuffle

import java.io.OutputStream
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{MemoryBlockObjectWriter, ShuffleBlockId}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.{Logging, SparkConf, SparkEnv}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by dikei on 4/20/16.
  */
class MemoryShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver with Logging {

  private lazy val blockManager = SparkEnv.get.blockManager
  private val shuffleToBlockId = new mutable.HashMap[Int, mutable.ArrayBuffer[ShuffleBlockId]]()
  private val bufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  /**
    * Contains all the state related to a particular shuffle.
    */
  private class ShuffleState(val numReducers: Int) {
    /**
      * The mapIds of all map tasks completed on this Executor for this shuffle.
      */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]
  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
    * Retrieve the data for the specified block. If the data for that block is not available,
    * throws an unspecified exception.
    */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val inMem = blockManager.memoryStore.getBytes(blockId)
    inMem match {
      case Some(byteBuffer) => new NioManagedBuffer(byteBuffer)
      case _ =>
        try {
          val onDisk = blockManager.diskStore.getBytes(blockId)
          onDisk match {
            case Some(byteBuffer) => new NioManagedBuffer(byteBuffer)
            case _ =>
              logError(s"No data available for $blockId")
              throw new Exception(s"No data available for $blockId")
          }
        } catch {
          case e: Exception =>
            log.error("Exception reading block from disk", e)
            throw e
        }
    }
  }

  override def stop(): Unit = {
    metadataCleaner.cancel()
  }

  def getWriters(
      shuffleId: Int,
      mapId: Int,
      numReducers: Int,
      serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics): Array[MemoryBlockObjectWriter] = {
    shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numReducers))
    val serializerInstance = serializer.newInstance()
    Array.tabulate[MemoryBlockObjectWriter](numReducers) { bucketId =>
      val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
      shuffleToBlockId.getOrElseUpdate(
        shuffleId,
        new mutable.ArrayBuffer[ShuffleBlockId]()
      ) += blockId
      val compression: OutputStream => OutputStream = blockManager.wrapForCompression(blockId, _)
      val syncWrites = conf.getBoolean("spark.shuffle.sync", defaultValue = false)
      new MemoryBlockObjectWriter(
        serializerInstance,
        bufferSize,
        compression,
        syncWrites,
        writeMetrics,
        blockId)
    }
  }

  def releaseShuffle(shuffleId: ShuffleId, mapId: Int): Unit = {
    val shuffleState = shuffleStates(shuffleId)
    shuffleState.completedMapTasks.add(mapId)
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle. */
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    log.info(s"Removing shuffle data for $shuffleId")
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        for (mapId <- state.completedMapTasks.asScala; reduceId <- 0 until state.numReducers) {
          val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
          if (!blockManager.memoryStore.remove(blockId)) {
            blockManager.diskStore.remove(blockId)
          }
        }
        true
      case None =>
        logInfo("Could not find shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }
}
