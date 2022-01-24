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

import okio.SegmentPool.LOCK
import okio.SegmentPool.recycle
import okio.SegmentPool.take
import java.util.concurrent.atomic.AtomicReference

/**
 * This class pools segments in a lock-free singly-linked stack. Though this code is lock-free it
 * does use a sentinel [LOCK] value to defend against races. Conflicted operations are not retried,
 * so there is no chance of blocking despite the term "lock".
 *
 * On [take], a caller swaps the stack's next pointer with the [LOCK] sentinel. If the stack was
 * not already locked, the caller replaces the head node with its successor.
 *
 * On [recycle], a caller swaps the head with a new node whose successor is the replaced head.
 *
 * On conflict, operations succeed, but segments are not pushed into the stack. For example, a
 * [take] that loses a race allocates a new segment regardless of the pool size. A [recycle] call
 * that loses a race will not increase the size of the pool. Under significant contention, this pool
 * will have fewer hits and the VM will do more GC and zero filling of arrays.
 *
 * This tracks the number of bytes in each linked list in its [Segment.limit] property. Each element
 * has a limit that's one segment size greater than its successor element. The maximum size of the
 * pool is a product of [MAX_SIZE] and [HASH_BUCKET_COUNT].
 */
internal actual object SegmentPool {
  /** The maximum number of bytes to pool per hash bucket. */
  // TODO: Is 64 KiB a good maximum size? Do we ever have that many idle segments?
  actual val MAX_SIZE = 64 * 1024 // 64 KiB.

  // Singleton, this is equivalent to the static field in the Singleton class in Java.
  /** A sentinel segment to indicate that the linked list is currently being modified. */
  private val LOCK = Segment(ByteArray(0), pos = 0, limit = 0, shared = false, owner = false)

  /**
   * The number of hash buckets. This number needs to balance keeping the pool small and contention
   * low. We use the number of processors rounded up to the nearest power of two. For example a
   * machine with 6 cores will have 8 hash buckets.
   */
  private val HASH_BUCKET_COUNT =
    Integer.highestOneBit(Runtime.getRuntime().availableProcessors() * 2 - 1)

  /**
   * Hash buckets each contain a singly-linked list of segments. The index/key is a hash function of
   * thread ID because it may reduce contention or increase locality.
   *
   * We don't use [ThreadLocal] because we don't know how many threads the host process has and we
   * don't want to leak memory for the duration of a thread's life.
   */
  private val hashBuckets: Array<AtomicReference<Segment?>> = Array(HASH_BUCKET_COUNT) {
    AtomicReference<Segment?>() // null value implies an empty bucket
  }

  actual val byteCount: Int
    get() {
      val first = firstRef().get() ?: return 0
      return first.limit
    }

  /**
   * Just like Handler#obtain, retrieve the element from head.
   */
  @JvmStatic
  actual fun take(): Segment {
    val firstRef = firstRef()

    // Atomically sets to the LOCK and returns the old value.
    val first = firstRef.getAndSet(LOCK)
    when {
      first === LOCK -> {
        // We didn't acquire the lock. Don't take a pooled segment.
        return Segment()
      }
      first == null -> {
        // We acquired the lock but the pool was empty. Unlock and return a new segment.
        firstRef.set(null)
        return Segment()
      }
      else -> {
        // We acquired the lock and the pool was not empty. Pop the first element and return it.
        firstRef.set(first.next)
        first.next = null
        first.limit = 0
        return first
      }
    }
  }

  @JvmStatic
  actual fun recycle(segment: Segment) {
    require(segment.next == null && segment.prev == null)
    // 标记为shared的Segment无法被回收
    if (segment.shared) return // This segment cannot be recycled.

    val firstRef = firstRef()

    val first = firstRef.get()
    if (first === LOCK) return // A take() is currently in progress.
    val firstLimit = first?.limit ?: 0
    if (firstLimit >= MAX_SIZE) return // Pool is full.

    // recycle to the head.
    segment.next = first
    segment.pos = 0
    segment.limit = firstLimit + Segment.SIZE

    // Not to recycle the segment when CAS operation fails.
    // If we lost a race with another operation, don't recycle this segment.
    if (!firstRef.compareAndSet(first, segment)) {
      segment.next = null // Don't leak a reference in the pool either!
    }
  }

  /**
   * 根据当前线程id找到响应的hash bucket
   */
  private fun firstRef(): AtomicReference<Segment?> {
    // Get a value in [0..HASH_BUCKET_COUNT) based on the current thread.

    // HASH_BUCKED_COUNT = pow(2, n)

    // 这里的and计算相当于模运算，因为(HASH_BUCKET_COUNT - 1)的结果用bit表示与HASH_BUCKET_COUNT
    // 的bit表示相比，除最高位为0之外其他都为1，这样按位与的结果就相当于模运算了
    // 例如：
    //    28    %     8     =     4
    // 00011100 & 0000 0111 = 0000 0100 = 4

    // e.g.
    // HASH_BUCKED_COUNT = 8, id = 135 => 135 % 7 = 8
    // hash散列表
    val hashBucket = (Thread.currentThread().id and (HASH_BUCKET_COUNT - 1L)).toInt()
    return hashBuckets[hashBucket]
  }
}
