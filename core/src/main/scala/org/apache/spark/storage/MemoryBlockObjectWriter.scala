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

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.Logging
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects to byte array
 */
private[spark] class MemoryBlockObjectWriter(
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  /**
    * Add getByteBuffer to avoid copying buf when calling toByteArray
    */
  private class ZeroCopyByteArrayOutputStream extends ByteArrayOutputStream {
    def getByteBuffer: ByteBuffer = {
      ByteBuffer.wrap(buf, 0, count)
    }
  }

  private var bs: OutputStream = null
  private var bos: ZeroCopyByteArrayOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var hasBeenClosed = false
  private var result: ByteBuffer = null

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0
  private var reportedPosition = 0

  def open(): MemoryBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    bos = new ZeroCopyByteArrayOutputStream()
    ts = new TimeTrackingOutputStream(writeMetrics, bos)
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializerInstance.serializeStream(bs)
    initialized = true
    this
  }

  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        if (syncWrites) {
          // Force outstanding writes to disk and track how long it takes
          objOut.flush()
          val start = System.nanoTime()
          writeMetrics.incShuffleWriteTime(System.nanoTime() - start)
        }
      } {
        objOut.close()
      }

      result = bos.getByteBuffer
      bs = null
      bos = null
      ts = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
  }

  def isOpen: Boolean = objOut != null

  /**
   * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!initialized) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incShuffleRecordsWritten(1)

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten()
    }
  }

  def getResult: ByteBuffer = {
    result
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = bos.size()
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
