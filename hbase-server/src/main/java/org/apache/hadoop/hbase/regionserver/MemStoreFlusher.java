/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Thread that flushes cache on request NOTE: This class extends Thread rather than Chore because
 * the sleep time can be interrupted when there is something to do, rather than the Chore sleep time
 * which is invariant.
 * @see FlushRequester
 */
@InterfaceAudience.Private
public class MemStoreFlusher implements FlushRequester, ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(MemStoreFlusher.class);

  private Configuration conf;
  // These two data members go together. Any entry in the one must have
  // a corresponding entry in the other.
  private final BlockingQueue<FlushQueueEntry> flushQueue = new DelayQueue<>();
  protected final Map<Region, FlushRegionEntry> regionsInQueue = new HashMap<>();
  private AtomicBoolean wakeupPending = new AtomicBoolean();

  private final long threadWakeFrequency;
  private final HRegionServer server;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Object blockSignal = new Object();

  private long blockingWaitTime;
  private final LongAdder updatesBlockedMsHighWater = new LongAdder();

  private FlushHandler[] flushHandlers;

  private final AtomicInteger flusherIdGen = new AtomicInteger();

  private ThreadFactory flusherThreadFactory;

  private List<FlushRequestListener> flushRequestListeners = new ArrayList<>(1);

  /**
   * Singleton instance inserted into flush queue used for signaling.
   */
  private static final FlushQueueEntry WAKEUPFLUSH_INSTANCE = new FlushQueueEntry() {
    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed o) {
      return -1;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public int hashCode() {
      return 42;
    }
  };

  /**
   *   */
  public MemStoreFlusher(final Configuration conf, final HRegionServer server) {
    super();
    this.conf = conf;
    this.server = server;
    this.threadWakeFrequency = conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.blockingWaitTime = conf.getInt("hbase.hstore.blockingWaitTime", 90000);
    int handlerCount = 0;
    if (server != null) {
      handlerCount = getHandlerCount(conf);
      LOG.info("globalMemStoreLimit="
        + TraditionalBinaryPrefix
          .long2String(this.server.getRegionServerAccounting().getGlobalMemStoreLimit(), "", 1)
        + ", globalMemStoreLimitLowMark="
        + TraditionalBinaryPrefix.long2String(
          this.server.getRegionServerAccounting().getGlobalMemStoreLimitLowMark(), "", 1)
        + ", Offheap=" + (this.server.getRegionServerAccounting().isOffheap()));
    }
    this.flushHandlers = new FlushHandler[handlerCount];
  }

  public LongAdder getUpdatesBlockedMsHighWater() {
    return this.updatesBlockedMsHighWater;
  }

  /**
   * The memstore across all regions has exceeded the low water mark. Pick one region to flush and
   * flush it synchronously (this is called from the flush thread)
   * @return true if successful
   */
  private boolean flushOneForGlobalPressure(FlushType flushType) {
    SortedMap<Long, HRegion> regionsBySize = null;
    switch (flushType) {
      case ABOVE_OFFHEAP_HIGHER_MARK:
      case ABOVE_OFFHEAP_LOWER_MARK:
        regionsBySize = server.getCopyOfOnlineRegionsSortedByOffHeapSize();
        break;
      case ABOVE_ONHEAP_HIGHER_MARK:
      case ABOVE_ONHEAP_LOWER_MARK:
      default:
        regionsBySize = server.getCopyOfOnlineRegionsSortedByOnHeapSize();
    }
    Set<HRegion> excludedRegions = new HashSet<>();

    double secondaryMultiplier =
      ServerRegionReplicaUtil.getRegionReplicaStoreFileRefreshMultiplier(conf);

    boolean flushedOne = false;
    while (!flushedOne) {
      // Find the biggest region that doesn't have too many storefiles (might be null!)
      HRegion bestFlushableRegion = getBiggestMemStoreRegion(regionsBySize, excludedRegions, true);
      // Find the biggest region, total, even if it might have too many flushes.
      HRegion bestAnyRegion = getBiggestMemStoreRegion(regionsBySize, excludedRegions, false);
      // Find the biggest region that is a secondary region
      HRegion bestRegionReplica = getBiggestMemStoreOfRegionReplica(regionsBySize, excludedRegions);
      if (bestAnyRegion == null) {
        // If bestAnyRegion is null, assign replica. It may be null too. Next step is check for null
        bestAnyRegion = bestRegionReplica;
      }
      if (bestAnyRegion == null) {
        LOG.error("Above memory mark but there are no flushable regions!");
        return false;
      }

      HRegion regionToFlush;
      long bestAnyRegionSize;
      long bestFlushableRegionSize;
      switch (flushType) {
        case ABOVE_OFFHEAP_HIGHER_MARK:
        case ABOVE_OFFHEAP_LOWER_MARK:
          bestAnyRegionSize = bestAnyRegion.getMemStoreOffHeapSize();
          bestFlushableRegionSize = getMemStoreOffHeapSize(bestFlushableRegion);
          break;

        case ABOVE_ONHEAP_HIGHER_MARK:
        case ABOVE_ONHEAP_LOWER_MARK:
          bestAnyRegionSize = bestAnyRegion.getMemStoreHeapSize();
          bestFlushableRegionSize = getMemStoreHeapSize(bestFlushableRegion);
          break;

        default:
          bestAnyRegionSize = bestAnyRegion.getMemStoreDataSize();
          bestFlushableRegionSize = getMemStoreDataSize(bestFlushableRegion);
      }
      if (bestAnyRegionSize > 2 * bestFlushableRegionSize) {
        // Even if it's not supposed to be flushed, pick a region if it's more than twice
        // as big as the best flushable one - otherwise when we're under pressure we make
        // lots of little flushes and cause lots of compactions, etc, which just makes
        // life worse!
        if (LOG.isDebugEnabled()) {
          LOG.debug("Under global heap pressure: " + "Region "
            + bestAnyRegion.getRegionInfo().getRegionNameAsString() + " has too many "
            + "store files, but is " + TraditionalBinaryPrefix.long2String(bestAnyRegionSize, "", 1)
            + " vs best flushable region's "
            + TraditionalBinaryPrefix.long2String(bestFlushableRegionSize, "", 1)
            + ". Choosing the bigger.");
        }
        regionToFlush = bestAnyRegion;
      } else {
        if (bestFlushableRegion == null) {
          regionToFlush = bestAnyRegion;
        } else {
          regionToFlush = bestFlushableRegion;
        }
      }

      long regionToFlushSize;
      long bestRegionReplicaSize;
      switch (flushType) {
        case ABOVE_OFFHEAP_HIGHER_MARK:
        case ABOVE_OFFHEAP_LOWER_MARK:
          regionToFlushSize = regionToFlush.getMemStoreOffHeapSize();
          bestRegionReplicaSize = getMemStoreOffHeapSize(bestRegionReplica);
          break;

        case ABOVE_ONHEAP_HIGHER_MARK:
        case ABOVE_ONHEAP_LOWER_MARK:
          regionToFlushSize = regionToFlush.getMemStoreHeapSize();
          bestRegionReplicaSize = getMemStoreHeapSize(bestRegionReplica);
          break;

        default:
          regionToFlushSize = regionToFlush.getMemStoreDataSize();
          bestRegionReplicaSize = getMemStoreDataSize(bestRegionReplica);
      }

      if ((regionToFlush == null || regionToFlushSize == 0) && bestRegionReplicaSize == 0) {
        // A concurrency issue (such as splitting region) may happen such that the online region
        // seen by getCopyOfOnlineRegionsSortedByXX() method is no longer eligible to
        // getBiggestMemStoreRegion(). This means that we can come out of the loop
        LOG.debug("Above memory mark but there is no flushable region");
        return false;
      }

      if (
        regionToFlush == null || (bestRegionReplica != null
          && ServerRegionReplicaUtil.isRegionReplicaStoreFileRefreshEnabled(conf)
          && (bestRegionReplicaSize > secondaryMultiplier * regionToFlushSize))
      ) {
        LOG.info("Refreshing storefiles of region " + bestRegionReplica
          + " due to global heap pressure. Total memstore off heap size="
          + TraditionalBinaryPrefix
            .long2String(server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(), "", 1)
          + " memstore heap size=" + TraditionalBinaryPrefix
            .long2String(server.getRegionServerAccounting().getGlobalMemStoreHeapSize(), "", 1));
        flushedOne = refreshStoreFilesAndReclaimMemory(bestRegionReplica);
        if (!flushedOne) {
          LOG.info("Excluding secondary region " + bestRegionReplica
            + " - trying to find a different region to refresh files.");
          excludedRegions.add(bestRegionReplica);
        }
      } else {
        LOG.info("Flush of region " + regionToFlush + " due to global heap pressure. "
          + "Flush type=" + flushType.toString() + ", Total Memstore Heap size="
          + TraditionalBinaryPrefix
            .long2String(server.getRegionServerAccounting().getGlobalMemStoreHeapSize(), "", 1)
          + ", Total Memstore Off-Heap size="
          + TraditionalBinaryPrefix
            .long2String(server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(), "", 1)
          + ", Region memstore size="
          + TraditionalBinaryPrefix.long2String(regionToFlushSize, "", 1));
        flushedOne = flushRegion(regionToFlush, true, null, FlushLifeCycleTracker.DUMMY);

        if (!flushedOne) {
          LOG.info("Excluding unflushable region " + regionToFlush
            + " - trying to find a different region to flush.");
          excludedRegions.add(regionToFlush);
        }
      }
    }
    return true;
  }

  /** Returns Return memstore offheap size or null if <code>r</code> is null */
  private static long getMemStoreOffHeapSize(HRegion r) {
    return r == null ? 0 : r.getMemStoreOffHeapSize();
  }

  /** Returns Return memstore heap size or null if <code>r</code> is null */
  private static long getMemStoreHeapSize(HRegion r) {
    return r == null ? 0 : r.getMemStoreHeapSize();
  }

  /** Returns Return memstore data size or null if <code>r</code> is null */
  private static long getMemStoreDataSize(HRegion r) {
    return r == null ? 0 : r.getMemStoreDataSize();
  }

  private class FlushHandler extends Thread {

    private final AtomicBoolean running = new AtomicBoolean(true);

    private FlushHandler(String name) {
      super(name);
    }

    @Override
    public void run() {
      while (!server.isStopped() && running.get()) {
        FlushQueueEntry fqe = null;
        try {
          wakeupPending.set(false); // allow someone to wake us up again
          fqe = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
          if (fqe == null || fqe == WAKEUPFLUSH_INSTANCE) {
            FlushType type = isAboveLowWaterMark();
            if (type != FlushType.NORMAL) {
              LOG.debug("Flush thread woke up because memory above low water="
                + TraditionalBinaryPrefix.long2String(
                  server.getRegionServerAccounting().getGlobalMemStoreLimitLowMark(), "", 1));
              // For offheap memstore, even if the lower water mark was breached due to heap
              // overhead
              // we still select the regions based on the region's memstore data size.
              // TODO : If we want to decide based on heap over head it can be done without tracking
              // it per region.
              if (!flushOneForGlobalPressure(type)) {
                // Wasn't able to flush any region, but we're above low water mark
                // This is unlikely to happen, but might happen when closing the
                // entire server - another thread is flushing regions. We'll just
                // sleep a little bit to avoid spinning, and then pretend that
                // we flushed one, so anyone blocked will check again
                Thread.sleep(1000);
                wakeUpIfBlocking();
              }
              // Enqueue another one of these tokens so we'll wake up again
              wakeupFlushThread();
            }
            continue;
          }
          FlushRegionEntry fre = (FlushRegionEntry) fqe;
          if (!flushRegion(fre)) {
            break;
          }
        } catch (InterruptedException ex) {
          continue;
        } catch (ConcurrentModificationException ex) {
          continue;
        } catch (Exception ex) {
          LOG.error("Cache flusher failed for entry " + fqe, ex);
          if (!server.checkFileSystem()) {
            break;
          }
        }
      }

      if (server.isStopped()) {
        synchronized (regionsInQueue) {
          regionsInQueue.clear();
          flushQueue.clear();
        }

        // Signal anyone waiting, so they see the close flag
        wakeUpIfBlocking();
      }
      LOG.info(getName() + " exiting");
    }

    public void shutdown() {
      if (!running.compareAndSet(true, false)) {
        LOG.warn("{} is already signaled to shutdown", getName());
      }
    }
  }

  private void wakeupFlushThread() {
    if (wakeupPending.compareAndSet(false, true)) {
      flushQueue.add(WAKEUPFLUSH_INSTANCE);
    }
  }

  private HRegion getBiggestMemStoreRegion(SortedMap<Long, HRegion> regionsBySize,
    Set<HRegion> excludedRegions, boolean checkStoreFileCount) {
    synchronized (regionsInQueue) {
      for (HRegion region : regionsBySize.values()) {
        if (excludedRegions.contains(region)) {
          continue;
        }

        if (region.writestate.flushing || !region.writestate.writesEnabled) {
          continue;
        }

        if (checkStoreFileCount && isTooManyStoreFiles(region)) {
          continue;
        }
        return region;
      }
    }
    return null;
  }

  private HRegion getBiggestMemStoreOfRegionReplica(SortedMap<Long, HRegion> regionsBySize,
    Set<HRegion> excludedRegions) {
    synchronized (regionsInQueue) {
      for (HRegion region : regionsBySize.values()) {
        if (excludedRegions.contains(region)) {
          continue;
        }

        if (RegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
          continue;
        }

        return region;
      }
    }
    return null;
  }

  private boolean refreshStoreFilesAndReclaimMemory(Region region) {
    try {
      return region.refreshStoreFiles();
    } catch (IOException e) {
      LOG.warn("Refreshing store files failed with exception", e);
    }
    return false;
  }

  /**
   * Return the FlushType if global memory usage is above the high watermark
   */
  private FlushType isAboveHighWaterMark() {
    return server.getRegionServerAccounting().isAboveHighWaterMark();
  }

  /**
   * Return the FlushType if we're above the low watermark
   */
  private FlushType isAboveLowWaterMark() {
    return server.getRegionServerAccounting().isAboveLowWaterMark();
  }

  @Override
  public boolean requestFlush(HRegion r, FlushLifeCycleTracker tracker) {
    return this.requestFlush(r, null, tracker);
  }

  @Override
  public boolean requestFlush(HRegion r, List<byte[]> families, FlushLifeCycleTracker tracker) {
    synchronized (regionsInQueue) {
      FlushRegionEntry existFqe = regionsInQueue.get(r);
      if (existFqe != null) {
        // if a delayed one exists and not reach the time to execute, just remove it
        if (existFqe.isDelay() && existFqe.whenToExpire > EnvironmentEdgeManager.currentTime()) {
          LOG.info("Remove the existing delayed flush entry for {}, "
            + "because we need to flush it immediately", r);
          this.regionsInQueue.remove(r);
          this.flushQueue.remove(existFqe);
          r.decrementFlushesQueuedCount();
        } else {
          tracker.notExecuted("Flush already requested on " + r);
          return false;
        }
      }

      // This entry has no delay so it will be added at the top of the flush
      // queue. It'll come out near immediately.
      FlushRegionEntry fqe = new FlushRegionEntry(r, families, tracker);
      this.regionsInQueue.put(r, fqe);
      this.flushQueue.add(fqe);
      r.incrementFlushesQueuedCount();
      return true;
    }
  }

  @Override
  public boolean requestDelayedFlush(HRegion r, long delay) {
    synchronized (regionsInQueue) {
      if (!regionsInQueue.containsKey(r)) {
        // This entry has some delay
        FlushRegionEntry fqe = new FlushRegionEntry(r, null, FlushLifeCycleTracker.DUMMY);
        fqe.requeue(delay);
        this.regionsInQueue.put(r, fqe);
        this.flushQueue.add(fqe);
        r.incrementFlushesQueuedCount();
        return true;
      }
      return false;
    }
  }

  public int getFlushQueueSize() {
    return flushQueue.size();
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    lock.writeLock().lock();
    try {
      for (FlushHandler flushHandler : flushHandlers) {
        if (flushHandler != null) {
          flushHandler.interrupt();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  synchronized void start(UncaughtExceptionHandler eh) {
    this.flusherThreadFactory =
      new ThreadFactoryBuilder().setDaemon(true).setUncaughtExceptionHandler(eh).build();
    lock.readLock().lock();
    try {
      startFlushHandlerThreads(flushHandlers, 0, flushHandlers.length);
    } finally {
      lock.readLock().unlock();
    }
  }

  boolean isAlive() {
    lock.readLock().lock();
    try {
      for (FlushHandler flushHandler : flushHandlers) {
        if (flushHandler != null && flushHandler.isAlive()) {
          return true;
        }
      }
      return false;
    } finally {
      lock.readLock().unlock();
    }
  }

  void shutdown() {
    lock.readLock().lock();
    try {
      for (FlushHandler flushHandler : flushHandlers) {
        if (flushHandler != null) {
          Threads.shutdown(flushHandler);
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * A flushRegion that checks store file count. If too many, puts the flush on delay queue to retry
   * later.
   * @return true if the region was successfully flushed, false otherwise. If false, there will be
   *         accompanying log messages explaining why the region was not flushed.
   */
  private boolean flushRegion(final FlushRegionEntry fqe) {
    HRegion region = fqe.region;
    if (!region.getRegionInfo().isMetaRegion() && isTooManyStoreFiles(region)) {
      if (fqe.isMaximumWait(this.blockingWaitTime)) {
        LOG.info("Waited " + (EnvironmentEdgeManager.currentTime() - fqe.createTime)
          + "ms on a compaction to clean up 'too many store files'; waited "
          + "long enough... proceeding with flush of "
          + region.getRegionInfo().getRegionNameAsString());
      } else {
        // If this is first time we've been put off, then emit a log message.
        if (fqe.getRequeueCount() <= 0) {
          // Note: We don't impose blockingStoreFiles constraint on meta regions
          LOG.warn("{} has too many store files({}); delaying flush up to {} ms",
            region.getRegionInfo().getEncodedName(), getStoreFileCount(region),
            this.blockingWaitTime);
          final CompactSplit compactSplitThread = server.getCompactSplitThread();
          if (!compactSplitThread.requestSplit(region)) {
            try {
              compactSplitThread.requestSystemCompaction(region, Thread.currentThread().getName());
            } catch (IOException e) {
              e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
              LOG.error("Cache flush failed for region "
                + Bytes.toStringBinary(region.getRegionInfo().getRegionName()), e);
            }
          }
        }

        // Put back on the queue. Have it come back out of the queue
        // after a delay of this.blockingWaitTime / 100 ms.
        this.flushQueue.add(fqe.requeue(this.blockingWaitTime / 100));
        // Tell a lie, it's not flushed but it's ok
        return true;
      }
    }
    return flushRegion(region, false, fqe.families, fqe.getTracker());
  }

  /**
   * Flush a region.
   * @param region         Region to flush.
   * @param emergencyFlush Set if we are being force flushed. If true the region needs to be removed
   *                       from the flush queue. If false, when we were called from the main flusher
   *                       run loop and we got the entry to flush by calling poll on the flush queue
   *                       (which removed it).
   * @param families       stores of region to flush.
   * @return true if the region was successfully flushed, false otherwise. If false, there will be
   *         accompanying log messages explaining why the region was not flushed.
   */
  private boolean flushRegion(HRegion region, boolean emergencyFlush, List<byte[]> families,
    FlushLifeCycleTracker tracker) {
    synchronized (this.regionsInQueue) {
      FlushRegionEntry fqe = this.regionsInQueue.remove(region);
      // Use the start time of the FlushRegionEntry if available
      if (fqe != null && emergencyFlush) {
        // Need to remove from region from delay queue. When NOT an
        // emergencyFlush, then item was removed via a flushQueue.poll.
        flushQueue.remove(fqe);
      }
    }

    tracker.beforeExecution();
    lock.readLock().lock();
    final CompactSplit compactSplitThread = server.getCompactSplitThread();
    try {
      notifyFlushRequest(region, emergencyFlush);
      FlushResult flushResult = region.flushcache(families, false, tracker);
      boolean shouldCompact = flushResult.isCompactionNeeded();
      // We just want to check the size
      boolean shouldSplit = region.checkSplit().isPresent();
      if (shouldSplit) {
        compactSplitThread.requestSplit(region);
      } else if (shouldCompact) {
        compactSplitThread.requestSystemCompaction(region, Thread.currentThread().getName());
      }
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of wal
      // is required. Currently the only way to do this is a restart of
      // the server. Abort because hdfs is probably bad (HBASE-644 is a case
      // where hdfs was bad but passed the hdfs check).
      server.abort("Replay of WAL required. Forcing server shutdown", ex);
      return false;
    } catch (IOException ex) {
      ex = ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex;
      LOG.error("Cache flush failed" + (region != null
        ? (" for region " + Bytes.toStringBinary(region.getRegionInfo().getRegionName()))
        : ""), ex);
      if (!server.checkFileSystem()) {
        return false;
      }
    } finally {
      lock.readLock().unlock();
      wakeUpIfBlocking();
      tracker.afterExecution();
    }
    return true;
  }

  private void notifyFlushRequest(Region region, boolean emergencyFlush) {
    FlushType type = null;
    if (emergencyFlush) {
      type = isAboveHighWaterMark();
    }
    if (type == null) {
      type = isAboveLowWaterMark();
    }
    for (FlushRequestListener listener : flushRequestListeners) {
      listener.flushRequested(type, region);
    }
  }

  private void wakeUpIfBlocking() {
    synchronized (blockSignal) {
      blockSignal.notifyAll();
    }
  }

  private boolean isTooManyStoreFiles(Region region) {

    // When compaction is disabled, the region is flushable
    if (!region.getTableDescriptor().isCompactionEnabled()) {
      return false;
    }

    for (Store store : region.getStores()) {
      if (store.hasTooManyStoreFiles()) {
        return true;
      }
    }
    return false;
  }

  private int getStoreFileCount(Region region) {
    int count = 0;
    for (Store store : region.getStores()) {
      count += store.getStorefilesCount();
    }
    return count;
  }

  /**
   * Check if the regionserver's memstore memory usage is greater than the limit. If so, flush
   * regions with the biggest memstores until we're down to the lower limit. This method blocks
   * callers until we're down to a safe amount of memstore consumption.
   */
  public void reclaimMemStoreMemory() {
    Span span =
      TraceUtil.getGlobalTracer().spanBuilder("MemStoreFluser.reclaimMemStoreMemory").startSpan();
    try (Scope scope = span.makeCurrent()) {
      FlushType flushType = isAboveHighWaterMark();
      if (flushType != FlushType.NORMAL) {
        span.addEvent("Force Flush. We're above high water mark.");
        long start = EnvironmentEdgeManager.currentTime();
        long nextLogTimeMs = start;
        synchronized (this.blockSignal) {
          boolean blocked = false;
          long startTime = 0;
          boolean interrupted = false;
          try {
            flushType = isAboveHighWaterMark();
            while (flushType != FlushType.NORMAL && !server.isStopped()) {
              if (!blocked) {
                startTime = EnvironmentEdgeManager.currentTime();
                if (!server.getRegionServerAccounting().isOffheap()) {
                  logMsg("global memstore heapsize",
                    server.getRegionServerAccounting().getGlobalMemStoreHeapSize(),
                    server.getRegionServerAccounting().getGlobalMemStoreLimit());
                } else {
                  switch (flushType) {
                    case ABOVE_OFFHEAP_HIGHER_MARK:
                      logMsg("the global offheap memstore datasize",
                        server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(),
                        server.getRegionServerAccounting().getGlobalMemStoreLimit());
                      break;
                    case ABOVE_ONHEAP_HIGHER_MARK:
                      logMsg("global memstore heapsize",
                        server.getRegionServerAccounting().getGlobalMemStoreHeapSize(),
                        server.getRegionServerAccounting().getGlobalOnHeapMemStoreLimit());
                      break;
                    default:
                      break;
                  }
                }
              }
              blocked = true;
              wakeupFlushThread();
              try {
                // we should be able to wait forever, but we've seen a bug where
                // we miss a notify, so put a 5 second bound on it at least.
                blockSignal.wait(5 * 1000);
              } catch (InterruptedException ie) {
                LOG.warn("Interrupted while waiting");
                interrupted = true;
              }
              long nowMs = EnvironmentEdgeManager.currentTime();
              if (nowMs >= nextLogTimeMs) {
                LOG.warn("Memstore is above high water mark and block {} ms", nowMs - start);
                nextLogTimeMs = nowMs + 1000;
              }
              flushType = isAboveHighWaterMark();
            }
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }

          if (blocked) {
            final long totalTime = EnvironmentEdgeManager.currentTime() - startTime;
            if (totalTime > 0) {
              this.updatesBlockedMsHighWater.add(totalTime);
            }
            LOG.info("Unblocking updates for server " + server.toString());
          }
        }
      } else {
        flushType = isAboveLowWaterMark();
        if (flushType != FlushType.NORMAL) {
          wakeupFlushThread();
        }
        span.end();
      }
    }
  }

  private void logMsg(String type, long val, long max) {
    LOG.info("Blocking updates: {} {} is >= blocking {}", type,
      TraditionalBinaryPrefix.long2String(val, "", 1),
      TraditionalBinaryPrefix.long2String(max, "", 1));
  }

  @Override
  public String toString() {
    return "flush_queue=" + flushQueue.size();
  }

  public String dumpQueue() {
    StringBuilder queueList = new StringBuilder();
    queueList.append("Flush Queue Queue dump:\n");
    queueList.append("  Flush Queue:\n");
    java.util.Iterator<FlushQueueEntry> it = flushQueue.iterator();

    while (it.hasNext()) {
      queueList.append("    " + it.next().toString());
      queueList.append("\n");
    }

    return queueList.toString();
  }

  /**
   * Register a MemstoreFlushListener
   */
  @Override
  public void registerFlushRequestListener(final FlushRequestListener listener) {
    this.flushRequestListeners.add(listener);
  }

  /**
   * Unregister the listener from MemstoreFlushListeners
   * @return true when passed listener is unregistered successfully.
   */
  @Override
  public boolean unregisterFlushRequestListener(final FlushRequestListener listener) {
    return this.flushRequestListeners.remove(listener);
  }

  /**
   * Sets the global memstore limit to a new size.
   */
  @Override
  public void setGlobalMemStoreLimit(long globalMemStoreSize) {
    this.server.getRegionServerAccounting().setGlobalMemStoreLimits(globalMemStoreSize);
    reclaimMemStoreMemory();
  }

  interface FlushQueueEntry extends Delayed {
  }

  /**
   * Datastructure used in the flush queue. Holds region and retry count. Keeps tabs on how old this
   * object is. Implements {@link Delayed}. On construction, the delay is zero. When added to a
   * delay queue, we'll come out near immediately. Call {@link #requeue(long)} passing delay in
   * milliseconds before readding to delay queue if you want it to stay there a while.
   */
  static class FlushRegionEntry implements FlushQueueEntry {
    private final HRegion region;

    private final long createTime;
    private long whenToExpire;
    private int requeueCount = 0;

    private final List<byte[]> families;

    private final FlushLifeCycleTracker tracker;

    FlushRegionEntry(final HRegion r, List<byte[]> families, FlushLifeCycleTracker tracker) {
      this.region = r;
      this.createTime = EnvironmentEdgeManager.currentTime();
      this.whenToExpire = this.createTime;
      this.families = families;
      this.tracker = tracker;
    }

    /** Returns True if we have been delayed > <code>maximumWait</code> milliseconds. */
    public boolean isMaximumWait(final long maximumWait) {
      return (EnvironmentEdgeManager.currentTime() - this.createTime) > maximumWait;
    }

    /** Returns True if the entry is a delay flush task */
    protected boolean isDelay() {
      return this.whenToExpire > this.createTime;
    }

    /**
     * @return Count of times {@link #requeue(long)} was called; i.e this is number of times we've
     *         been requeued.
     */
    public int getRequeueCount() {
      return this.requeueCount;
    }

    public FlushLifeCycleTracker getTracker() {
      return tracker;
    }

    /**
     * @param when When to expire, when to come up out of the queue. Specify in milliseconds. This
     *             method adds EnvironmentEdgeManager.currentTime() to whatever you pass.
     * @return This.
     */
    public FlushRegionEntry requeue(final long when) {
      this.whenToExpire = EnvironmentEdgeManager.currentTime() + when;
      this.requeueCount++;
      return this;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.whenToExpire - EnvironmentEdgeManager.currentTime(),
        TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      // Delay is compared first. If there is a tie, compare region's hash code
      int ret =
        Long.valueOf(getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS))
          .intValue();
      if (ret != 0) {
        return ret;
      }
      FlushQueueEntry otherEntry = (FlushQueueEntry) other;
      return hashCode() - otherEntry.hashCode();
    }

    @Override
    public String toString() {
      return "[flush region " + Bytes.toStringBinary(region.getRegionInfo().getRegionName()) + "]";
    }

    @Override
    public int hashCode() {
      int hash = (int) getDelay(TimeUnit.MILLISECONDS);
      return hash ^ region.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      FlushRegionEntry other = (FlushRegionEntry) obj;
      if (
        !Bytes.equals(this.region.getRegionInfo().getRegionName(),
          other.region.getRegionInfo().getRegionName())
      ) {
        return false;
      }
      return compareTo(other) == 0;
    }
  }

  private int getHandlerCount(Configuration conf) {
    int handlerCount = conf.getInt("hbase.hstore.flusher.count", 2);
    if (handlerCount < 1) {
      LOG.warn(
        "hbase.hstore.flusher.count was configed to {} which is less than 1, " + "corrected to 1",
        handlerCount);
      handlerCount = 1;
    }
    return handlerCount;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    int newHandlerCount = getHandlerCount(newConf);
    if (newHandlerCount != flushHandlers.length) {
      LOG.info("update hbase.hstore.flusher.count from {} to {}", flushHandlers.length,
        newHandlerCount);
      lock.writeLock().lock();
      try {
        FlushHandler[] newFlushHandlers = Arrays.copyOf(flushHandlers, newHandlerCount);
        if (newHandlerCount > flushHandlers.length) {
          startFlushHandlerThreads(newFlushHandlers, flushHandlers.length, newFlushHandlers.length);
        } else {
          stopFlushHandlerThreads(flushHandlers, newHandlerCount, flushHandlers.length);
        }
        flusherIdGen.compareAndSet(flushHandlers.length, newFlushHandlers.length);
        this.flushHandlers = newFlushHandlers;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  private void startFlushHandlerThreads(FlushHandler[] flushHandlers, int start, int end) {
    if (flusherThreadFactory != null) {
      for (int i = start; i < end; i++) {
        flushHandlers[i] = new FlushHandler("MemStoreFlusher." + flusherIdGen.getAndIncrement());
        flusherThreadFactory.newThread(flushHandlers[i]);
        flushHandlers[i].start();
      }
    }
  }

  private void stopFlushHandlerThreads(FlushHandler[] flushHandlers, int start, int end) {
    for (int i = start; i < end; i++) {
      flushHandlers[i].shutdown();
      if (LOG.isDebugEnabled()) {
        LOG.debug("send shutdown signal to {}", flushHandlers[i].getName());
      }
    }
  }

  public int getFlusherCount() {
    return flusherIdGen.get();
  }
}
