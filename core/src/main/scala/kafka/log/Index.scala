/**
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

package kafka.log

import java.io.File

trait Index {
  def baseOffset: Long
  def maxIndexSize: Int
  protected def entrySize: Int

  def isFull: Boolean

  def file: File

  def maxEntries: Int

  def entries: Int

  def length: Long

  def updateParentDir(parentDir: File): Unit

  def resize(newSize: Int): Boolean

  def renameTo(f: File): Unit

  def flush(): Unit

  def deleteIfExists(): Boolean

  def trimToValidSize(): Unit

  def sizeInBytes: Int

  def close(): Unit

  def closeHandler(): Unit

  def sanityCheck(): Unit

  def truncate(): Unit

  def truncateTo(offset: Long): Unit

  def reset(): Unit

  def relativeOffset(offset: Long): Int

  def canAppendOffset(offset: Long): Boolean

  protected[log] def forceUnmap(): Unit
}
