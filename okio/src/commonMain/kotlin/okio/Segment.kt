/*
 * Copyright (C) 2014 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okio

import kotlin.jvm.JvmField

/**
 * A segment of a buffer.
 *
 * Each segment in a buffer is a circularly-linked list node referencing the following and
 * preceding segments in the buffer.
 *
 * Each segment in the pool is a singly-linked list node referencing the rest of segments in the
 * pool.
 *
 * The underlying byte arrays of segments may be shared between buffers and byte strings. When a
 * segment's byte array is shared the segment may not be recycled, nor may its byte data be changed.
 * The lone exception is that the owner segment is allowed to append to the segment, writing data at
 * `limit` and beyond. There is a single owning segment for each byte array. Positions,
 * limits, prev, and next references are not shared.
 */
internal class Segment {
  @JvmField val data: ByteArray

  // data的首个可读数据的偏移位置，同时也表示已读数据的长度，设计非常巧妙，类似Java nio
  /** The next byte of application data byte to read in this segment. */
  @JvmField var pos: Int = 0

  // data的首个可写数据的偏移位置，可表示已写入的数据长度
  // 个人理解，则data的剩余为写入的数据长度为：data.size - limit
  // 如果此Segment在SegmentPool中，则limit表示从当前Segment到之后的Segment的所有数据的长度之和
  /**
   * The first byte of available data ready to be written to.
   *
   * If the segment is free and linked in the segment pool, the field contains total
   * byte count of this and next segments.
   */
  @JvmField var limit: Int = 0

  /** True if other segments or byte strings use the same byte array. */
  @JvmField var shared: Boolean = false

  /** True if this segment owns the byte array and can append to it, extending `limit`. */
  @JvmField var owner: Boolean = false

  /** Next segment in a linked or circularly-linked list. */
  @JvmField var next: Segment? = null

  /** Previous segment in a circularly-linked list. */
  @JvmField var prev: Segment? = null

  constructor() {
    this.data = ByteArray(SIZE)
    this.owner = true
    this.shared = false
  }

  constructor(data: ByteArray, pos: Int, limit: Int, shared: Boolean, owner: Boolean) {
    this.data = data
    this.pos = pos
    this.limit = limit
    this.shared = shared
    this.owner = owner
  }

  /**
   * Returns a new segment that shares the underlying byte array with this. Adjusting pos and limit
   * are safe but writes are forbidden. This also marks the current segment as shared, which
   * prevents it from being pooled.
   */
  fun sharedCopy(): Segment {
    shared = true
    return Segment(data, pos, limit, true, false)
  }

  /** Returns a new segment that its own private copy of the underlying byte array.  */
  fun unsharedCopy() = Segment(data.copyOf(), pos, limit, false, true)

  /**
   * Removes this segment of a circularly-linked list and returns its successor.
   * Returns null if the list is now empty.
   */
  fun pop(): Segment? {
    val result = if (next !== this) next else null
    prev!!.next = next
    next!!.prev = prev
    next = null
    prev = null
    return result
  }

  /**
   * Appends `segment` after this segment in the circularly-linked list. Returns the pushed segment.
   */
  fun push(segment: Segment): Segment {
    segment.prev = this
    segment.next = next
    next!!.prev = segment
    next = segment
    return segment
  }

  /**
   * Splits this head of a circularly-linked list into two segments. The first segment contains the
   * data in `[pos..pos+byteCount)`. The second segment contains the data in
   * `[pos+byteCount..limit)`. This can be useful when moving partial segments from one buffer to
   * another.
   *
   * Returns the new head of the circularly-linked list.
   */
  fun split(byteCount: Int): Segment {
    // To check the argument is whether valid or not.
    require(byteCount > 0 && byteCount <= limit - pos) { "byteCount out of range" }
    val prefix: Segment

    // We have two competing performance goals:
    //  - Avoid copying data. We accomplish this by sharing segments.
    //  - Avoid short shared segments. These are bad for performance because they are readonly and
    //    may lead to long chains of short segments.
    // To balance these goals we only share segments when the copy will be large.
    if (byteCount >= SHARE_MINIMUM) {
      // copy a new Segment that shared the data field with the current one.
      prefix = sharedCopy()
    } else {
      prefix = SegmentPool.take()
      // copy [pos, pos + byteCount) to the given prefix.
      data.copyInto(prefix.data, startIndex = pos, endIndex = pos + byteCount)
    }

    prefix.limit = prefix.pos + byteCount
    pos += byteCount
    prev!!.push(prefix)
    return prefix
  }

  /**
   * Call this when the tail and its predecessor may both be less than half full. This will copy
   * data so that segments can be recycled.
   */
  fun compact() {
    check(prev !== this) { "cannot compact" }
    if (!prev!!.owner) return // Cannot compact: prev isn't writable.
    // 当前Segment的剩余未读数据长度
    val byteCount = limit - pos
    // 前一个Segment节点未写数据+已读数据长度之和
    val availableByteCount = SIZE - prev!!.limit + if (prev!!.shared) 0 else prev!!.pos
    // 剩余空间不够写入数据
    if (byteCount > availableByteCount) return // Cannot compact: not enough writable space.
    // 把当前的数据写到前一个Segment节点
    writeTo(prev!!, byteCount)
    // pop & recycle
    pop()
    SegmentPool.recycle(this)
  }

  /** Moves `byteCount` bytes from this segment to `sink`.  */
  fun writeTo(sink: Segment, byteCount: Int) {
    check(sink.owner) { "only owner can write" }
    // 超出sink的最大数据长度
    if (sink.limit + byteCount > SIZE) {
      // We can't fit byteCount bytes at the sink's current position. Shift sink first.
      if (sink.shared) throw IllegalArgumentException()
      if (sink.limit + byteCount - sink.pos > SIZE) throw IllegalArgumentException()
      // 丢弃已读数据，向前移动数据
      sink.data.copyInto(sink.data, startIndex = sink.pos, endIndex = sink.limit)
      sink.limit -= sink.pos
      sink.pos = 0
    }

    // 把当前Segment的数据写入到sink
    // 从sink的limit位置开始
    data.copyInto(
      sink.data, destinationOffset = sink.limit, startIndex = pos,
      endIndex = pos + byteCount
    )
    // 更新sink的limit
    sink.limit += byteCount
    // 更新当前Segment的首个可读数据的偏移地址
    pos += byteCount

    /**
     * 总结写入流程：
     * 首先设目标Segment为t，写入数据的长度为byteCount
     * 1. 只有owner才可以写入
     * 2. 写入数据到t前判断，若其剩余长度不够写入，即byteCount + t.limit > SIZE，则尝试移动t的数据：
     *
     *    如果t是共享的，则不能向前移动，throw IllegalArgumentException()
     *    如果t.limit - t.pos + byteCount > SIZE，则无法移动 -> throw IllegalStateException
     *
     *    否则把t中[pos, limit]的数据移动到[0, SIZE]处，也就是向前移动(因为pos之前的数据已经被读取了)
     *    t.pos = 0 t.limit -= t.pos
     *
     * 3. 写入数据到t
     */
  }

  companion object {
    // Segment的最大数据长度
    /** The size of all segments in bytes.  */
    const val SIZE = 8192

    /** Segments will be shared when doing so avoids `arraycopy()` of this many bytes.  */
    const val SHARE_MINIMUM = 1024
  }
}
