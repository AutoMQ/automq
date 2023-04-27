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
