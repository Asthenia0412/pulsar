/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * A thread-safe map to store pending acks in the consumer.
 *
 * <p>The locking solution is used for the draining hashes solution
 * to ensure that there's a consistent view of the pending acks. This is needed in the DrainingHashesTracker
 * to ensure that the reference counts are consistent at all times.
 * Calling forEachAndClose will ensure that no more entries can be added,
 * therefore no other thread cannot send out entries while the forEachAndClose is being called.
 * remove is also locked to ensure that there aren't races in the removal of entries while forEachAndClose is
 * running.
 *
 * <h3>Data-structure notes</h3>
 *
 * <ul>
 *   <li><b>Outer map</b> ({@code ledgerId -> LedgerPendingAcks}) is still a {@link Long2ObjectRBTreeMap} because
 *       {@link #removeAllUpTo} and {@link #forEach} need a ledgerId-ordered walk.</li>
 *   <li><b>Inner structure</b> is replaced from a sorted {@code Long2LongAVLTreeMap} (O(log&nbsp;n)) to an
 *       unordered {@link Long2LongOpenHashMap} (O(1) average).</li>
 *   <li>A per-ledger {@link BitSet} serves as a <b>prefix index</b> so that {@link #removeAllUpTo} can find every
 *       entryId {@code <= markDeleteEntryId} in O(k) instead of the previous O(log&nbsp;n&nbsp;+&nbsp;m). The
 *       BitSet capacity is capped at {@link #BITSET_MAX_CAPACITY}; if an entryId exceeds that we fall back to a
 *       full map scan. For typical pending-ack windows (a few thousand entries per ledger) the BitSet footprint
 *       is around a kilobyte.</li>
 * </ul>
 */
public class PendingAcksMap {
    static final int PENDING_ACK_NOT_FOUND = PendingAckValues.NOT_FOUND;

    /**
     * Upper bound for the per-ledger {@link BitSet} capacity. EntryIds exceeding this value are handled
     * by falling back to a full map scan in {@link #removeAllUpTo}.
     */
    static final int BITSET_MAX_CAPACITY = 1_000_000;

    /**
     * Callback interface for handling the addition of pending acknowledgments.
     */
    public interface PendingAcksAddHandler {
        /**
         * Handle the addition of a pending acknowledgment.
         *
         * @param consumer      the consumer
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param stickyKeyHash the sticky key hash
         * @return true if the addition is allowed, false otherwise
         */
        boolean handleAdding(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash);
    }

    /**
     * Callback interface for handling the removal of pending acknowledgments.
     */
    public interface PendingAcksRemoveHandler {
        /**
         * Handle the removal of a pending acknowledgment.
         *
         * @param consumer      the consumer
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param stickyKeyHash the sticky key hash
         * @param closing       true if the pending ack is being removed because the map is being closed, false
         *                      otherwise
         */
        void handleRemoving(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash, boolean closing);
        /**
         * Start a batch of pending acknowledgment removals.
         */
        void startBatch();
        /**
         * End a batch of pending acknowledgment removals.
         */
        void endBatch();
    }

    /**
     * Callback interface for processing pending acknowledgments.
     */
    public interface PendingAcksConsumer {
        /**
         * Accept a pending acknowledgment.
         *
         * @param ledgerId          the ledger ID
         * @param entryId           the entry ID
         * @param remainingUnacked  the number of remaining unacked messages in this entry
         *                          (accounts for batch index level acknowledgments)
         * @param stickyKeyHash     the sticky key hash
         */
        void accept(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);
    }

    /**
     * Per-ledger pending-ack state. Pairs a primitive O(1) hash map with a BitSet prefix index.
     */
    static final class LedgerPendingAcks {
        /** O(1) packed-value storage keyed by entryId. */
        final Long2LongOpenHashMap entries;
        /**
         * BitSet-backed prefix index. Bit {@code i} is set iff {@code entries} contains an entryId equal to
         * {@code i}. {@code null} is used as a sentinel when the BitSet is known to be unusable (e.g. an
         * entryId was negative or exceeded {@link PendingAcksMap#BITSET_MAX_CAPACITY}) — in that case we fall
         * back to a full scan.
         */
        BitSet entryBits;

        LedgerPendingAcks() {
            this.entries = new Long2LongOpenHashMap();
            this.entries.defaultReturnValue(PendingAckValues.PACKED_NOT_FOUND);
            this.entryBits = new BitSet(BITSET_MAX_CAPACITY);
        }

        /** Returns {@code true} if this ledger still has any pending acks. */
        boolean isEmpty() {
            return entries.isEmpty();
        }

        int size() {
            return entries.size();
        }
    }

    private final Consumer consumer;

    /**
     * Pending acknowledgments grouped by ledger id. The outer map key is ledgerId and each inner structure is
     * a {@link LedgerPendingAcks} that stores entryId → packedValue in a primitive hash map plus a BitSet
     * prefix index.
     */
    private final Long2ObjectSortedMap<LedgerPendingAcks> pendingAcks;
    private final Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier;
    private final Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier;
    private final Lock readLock;
    private final Lock writeLock;
    private boolean closed = false;
    private volatile long size;

    PendingAcksMap(Consumer consumer, Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier,
                   Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier) {
        this.consumer = consumer;
        this.pendingAcks = new Long2ObjectRBTreeMap<>();
        this.pendingAcksAddHandlerSupplier = pendingAcksAddHandlerSupplier;
        this.pendingAcksRemoveHandlerSupplier = pendingAcksRemoveHandlerSupplier;
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.writeLock = readWriteLock.writeLock();
        this.readLock = readWriteLock.readLock();
    }

    /**
     * Add a pending ack to the map if it's allowed to send a message with the given sticky key hash.
     * If this method returns false, it means that the pending ack was not added, and it's not allowed to send a
     * message. In that case, the caller should not send a message and skip the entry.
     * The sending could be disallowed if the sticky key hash is blocked in the Key_Shared subscription.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param remainingUnacked the number of remaining unacked messages in this entry
     *                         (for batch entries with some indexes already acked, this may be less than batchSize)
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was added, and it's allowed to send a message, false otherwise
     */
    public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
        try {
            writeLock.lock();
            // prevent adding sticky hash to pending acks if the PendingAcksMap has already been closed
            // and there's a race condition between closing the consumer and sending new messages
            if (closed) {
                return false;
            }
            // prevent adding sticky hash to pending acks if it's already in draining hashes
            // to avoid any race conditions that would break consistency
            PendingAcksAddHandler pendingAcksAddHandler = pendingAcksAddHandlerSupplier.get();
            if (pendingAcksAddHandler != null
                    && !pendingAcksAddHandler.handleAdding(consumer, ledgerId, entryId, stickyKeyHash)) {
                return false;
            }
            LedgerPendingAcks ledgerPendingAcks =
                    pendingAcks.computeIfAbsent(ledgerId, k -> new LedgerPendingAcks());
            long packedValue = PendingAckValues.pack(remainingUnacked, stickyKeyHash);
            long previous = ledgerPendingAcks.entries.put(entryId, packedValue);
            if (PendingAckValues.isNotFound(previous)) {
                size++;
            }
            // maintain BitSet prefix index
            int bitIndex = bitIndexOf(entryId);
            if (bitIndex >= 0 && ledgerPendingAcks.entryBits != null) {
                ledgerPendingAcks.entryBits.set(bitIndex);
            } else if (ledgerPendingAcks.entryBits != null && entryId < 0) {
                // negative entryId → BitSet can't help for this entry; BitSet still useful for others
                // (we do NOT null it out — positive entryIds still benefit)
            } else {
                // entryId out of BitSet capacity — mark whole BitSet unusable
                ledgerPendingAcks.entryBits = null;
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get the size of the pending acks map.
     *
     * @return the size of the pending acks map
     */
    public long size() {
        return size;
    }

    /**
     * Iterate over all the pending acks and process them using the given processor.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEach(PendingAcksConsumer processor) {
        try {
            readLock.lock();
            processPendingAcks(processor);
        } finally {
            readLock.unlock();
        }
    }

    // this code uses for loops intentionally, don't refactor to use forEach
    private void processPendingAcks(PendingAcksConsumer processor) {
        for (Long2ObjectMap.Entry<LedgerPendingAcks> entry : pendingAcks.long2ObjectEntrySet()) {
            long ledgerId = entry.getLongKey();
            Long2LongOpenHashMap ledgerEntries = entry.getValue().entries;
            for (Long2LongMap.Entry pendingAckEntry : ledgerEntries.long2LongEntrySet()) {
                long packedValue = pendingAckEntry.getLongValue();
                processor.accept(ledgerId, pendingAckEntry.getLongKey(),
                        PendingAckValues.remainingUnacked(packedValue), PendingAckValues.stickyKeyHash(packedValue));
            }
        }
    }

    /**
     * Iterate over all the pending acks and close the map so that no more entries can be added.
     * All entries are removed.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClose(PendingAcksConsumer processor) {
        internalForEachAndClear(processor, true);
    }

    /**
     * Iterate over all the pending acks and clear the map.
     * Unlike {@link #forEachAndClose(PendingAcksConsumer)}, this method does not close the map,
     * so new entries can still be added after this method returns.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClear(PendingAcksConsumer processor) {
        internalForEachAndClear(processor, false);
    }

    private void internalForEachAndClear(PendingAcksConsumer processor, boolean close) {
        try {
            writeLock.lock();
            if (close) {
                closed = true;
            }
            PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
            if (pendingAcksRemoveHandler != null) {
                try {
                    pendingAcksRemoveHandler.startBatch();
                    processPendingAcks((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                        processor.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                        pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
                    });
                } finally {
                    pendingAcksRemoveHandler.endBatch();
                }
            } else {
                processPendingAcks(processor);
            }
            pendingAcks.clear();
            size = 0;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Check if the map contains a pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the map contains the pending ack, false otherwise
     */
    public boolean contains(long ledgerId, long entryId) {
        try {
            readLock.lock();
            LedgerPendingAcks ledgerState = pendingAcks.get(ledgerId);
            if (ledgerState == null) {
                return false;
            }
            return ledgerState.entries.containsKey(entryId);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the remaining unacked count for the given ledger ID and entry ID.
     *
     * @return the remaining unacked count, or {@link #PENDING_ACK_NOT_FOUND} if not found
     */
    int getRemainingUnacked(long ledgerId, long entryId) {
        try {
            readLock.lock();
            LedgerPendingAcks ledgerState = pendingAcks.get(ledgerId);
            if (ledgerState == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            long packedValue = ledgerState.entries.get(entryId);
            return PendingAckValues.isNotFound(packedValue)
                    ? PENDING_ACK_NOT_FOUND : PendingAckValues.remainingUnacked(packedValue);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove the pending ack for the given ledger ID, entry ID, batch size, and sticky key hash.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param batchSize the batch size
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerState = pendingAcks.get(ledgerId);
            if (batchSize < 0) {
                return false;
            }
            long expectedValue = PendingAckValues.pack(batchSize, stickyKeyHash);
            if (ledgerState == null || ledgerState.entries.get(entryId) != expectedValue) {
                return false;
            }
            ledgerState.entries.remove(entryId);
            clearBit(ledgerState, entryId);
            size--;
            handleRemovePendingAck(ledgerId, entryId, stickyKeyHash);
            if (ledgerState.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically update the remaining unacked count for a pending ack entry by subtracting the given delta.
     * Called from the ack handler after computing the number of batch indexes acknowledged in a partial ack.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param ackedDelta the number of batch indexes that were just acknowledged
     * @return true if the entry was found and updated, false otherwise
     */
    public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerState = pendingAcks.get(ledgerId);
            if (ledgerState == null) {
                return false;
            }
            long packedValue = ledgerState.entries.get(entryId);
            if (PendingAckValues.isNotFound(packedValue)) {
                return false;
            }
            int newRemaining = PendingAckValues.remainingUnacked(packedValue) - ackedDelta;
            if (newRemaining < 0) {
                return false;
            }
            ledgerState.entries.put(entryId,
                    PendingAckValues.pack(newRemaining, PendingAckValues.stickyKeyHash(packedValue)));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remove the pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerState = pendingAcks.get(ledgerId);
            if (ledgerState == null) {
                return false;
            }
            long removedEntry = ledgerState.entries.remove(entryId);
            if (PendingAckValues.isNotFound(removedEntry)) {
                return false;
            }
            clearBit(ledgerState, entryId);
            size--;
            handleRemovePendingAck(ledgerId, entryId, PendingAckValues.stickyKeyHash(removedEntry));
            if (ledgerState.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove and return the remaining unacked count for the given ledger ID and entry ID.
     *
     * @return the remaining unacked count, or {@link #PENDING_ACK_NOT_FOUND} if not found
     */
    int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerState = pendingAcks.get(ledgerId);
            if (ledgerState == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            long removedEntry = ledgerState.entries.remove(entryId);
            if (PendingAckValues.isNotFound(removedEntry)) {
                return PENDING_ACK_NOT_FOUND;
            }
            clearBit(ledgerState, entryId);
            size--;
            handleRemovePendingAck(ledgerId, entryId, PendingAckValues.stickyKeyHash(removedEntry));
            if (ledgerState.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return PendingAckValues.remainingUnacked(removedEntry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remove all pending acks up to the given ledger ID and entry ID, invoking a callback for each removed entry.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     * @param removedEntryCallback optional callback invoked for each removed entry (within the write lock),
     *                             receiving ledgerId, entryId, batchSize, and stickyKeyHash
     */
    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId,
                             PendingAcksConsumer removedEntryCallback) {
        internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, false, removedEntryCallback);
    }

    /**
     * Removes all pending acknowledgments up to the specified ledger ID and entry ID.
     *
     * <p>ReadWriteLock doesn't support upgrading from read lock to write lock.
     * This method first checks if there's anything to remove using a read lock and if there is, exits
     * and retries with a write lock to make the removals.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     * @param useWriteLock true if the method should use a write lock, false otherwise
     * @param removedEntryCallback optional callback invoked for each removed entry (within the write lock)
     */
    private void internalRemoveAllUpTo(long markDeleteLedgerId, long markDeleteEntryId, boolean useWriteLock,
                                      PendingAcksConsumer removedEntryCallback) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        // track if the write lock was acquired
        boolean acquiredWriteLock = false;
        // track if a batch was started
        boolean batchStarted = false;
        // track if the method should retry with a write lock
        boolean retryWithWriteLock = false;
        try {
            if (useWriteLock) {
                writeLock.lock();
                acquiredWriteLock = true;
            } else {
                readLock.lock();
            }
            ObjectBidirectionalIterator<Long2ObjectMap.Entry<LedgerPendingAcks>> ledgerMapIterator =
                    pendingAcks.headMap(markDeleteLedgerId + 1).long2ObjectEntrySet().iterator();
            while (ledgerMapIterator.hasNext()) {
                Long2ObjectMap.Entry<LedgerPendingAcks> entry = ledgerMapIterator.next();
                long ledgerId = entry.getLongKey();
                LedgerPendingAcks ledgerState = entry.getValue();
                boolean isTargetLedger = (ledgerId == markDeleteLedgerId);

                // Collect entries to remove
                long[] entriesToRemove;
                if (isTargetLedger) {
                    entriesToRemove = collectEntryIdsUpTo(ledgerState, markDeleteEntryId);
                } else {
                    // This ledger is entirely below the mark-delete ledger — remove everything
                    entriesToRemove = new long[ledgerState.entries.size()];
                    int idx = 0;
                    for (Long2LongMap.Entry e : ledgerState.entries.long2LongEntrySet()) {
                        entriesToRemove[idx++] = e.getLongKey();
                    }
                }

                // Nothing to remove on this ledger — skip
                if (entriesToRemove.length == 0) {
                    continue;
                }

                if (!acquiredWriteLock) {
                    retryWithWriteLock = true;
                    return;
                }

                for (long entryId : entriesToRemove) {
                    long packedValue = ledgerState.entries.remove(entryId);
                    if (PendingAckValues.isNotFound(packedValue)) {
                        // Was removed by a concurrent writer between our read-scan and the write — skip
                        continue;
                    }
                    clearBit(ledgerState, entryId);
                    size--;
                    int batchSize = PendingAckValues.remainingUnacked(packedValue);
                    int stickyKeyHash = PendingAckValues.stickyKeyHash(packedValue);
                    if (pendingAcksRemoveHandler != null) {
                        if (!batchStarted) {
                            pendingAcksRemoveHandler.startBatch();
                            batchStarted = true;
                        }
                        pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
                    }
                    if (removedEntryCallback != null) {
                        removedEntryCallback.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                    }
                }

                if (ledgerState.isEmpty()) {
                    ledgerMapIterator.remove();
                }
            }
        } finally {
            if (batchStarted) {
                pendingAcksRemoveHandler.endBatch();
            }
            if (acquiredWriteLock) {
                writeLock.unlock();
            } else {
                readLock.unlock();
                if (retryWithWriteLock) {
                    internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, true, removedEntryCallback);
                }
            }
        }
    }

    /**
     * Collects entryIds {@code <= markDeleteEntryId} from the given ledger state.
     * Uses the BitSet prefix index when possible (O(k)) and falls back to a full map scan otherwise.
     *
     * @return a non-null array of entryIds to remove (may be empty)
     */
    private static long[] collectEntryIdsUpTo(LedgerPendingAcks ledgerState, long markDeleteEntryId) {
        BitSet bits = ledgerState.entryBits;
        if (bits != null && markDeleteEntryId >= 0 && markDeleteEntryId < BITSET_MAX_CAPACITY) {
            // BitSet path: O(k) where k is the number of entries ≤ markDeleteEntryId
            int limit = (int) markDeleteEntryId;
            int count = 0;
            // First pass: count
            for (int i = bits.nextSetBit(0); i >= 0 && i <= limit; i = bits.nextSetBit(i + 1)) {
                count++;
            }
            long[] result = new long[count];
            int idx = 0;
            for (int i = bits.nextSetBit(0); i >= 0 && i <= limit; i = bits.nextSetBit(i + 1)) {
                // verify entry is still present in the hash map (it could have been removed concurrently)
                if (!PendingAckValues.isNotFound(ledgerState.entries.get(i))) {
                    result[idx++] = i;
                }
            }
            if (idx < result.length) {
                // concurrent removals shrank the actual set — trim
                long[] trimmed = new long[idx];
                System.arraycopy(result, 0, trimmed, 0, idx);
                return trimmed;
            }
            return result;
        } else {
            // Fallback: full scan over the hash map (unordered, but caller doesn't care about order)
            java.util.ArrayList<Long> candidates = new java.util.ArrayList<>();
            for (Long2LongMap.Entry e : ledgerState.entries.long2LongEntrySet()) {
                long id = e.getLongKey();
                if (id <= markDeleteEntryId) {
                    candidates.add(id);
                }
            }
            long[] result = new long[candidates.size()];
            for (int i = 0; i < result.length; i++) {
                result[i] = candidates.get(i);
            }
            return result;
        }
    }

    /**
     * Converts an entryId to a BitSet index. Returns {@code -1} for negative entryIds.
     */
    private static int bitIndexOf(long entryId) {
        if (entryId < 0 || entryId >= BITSET_MAX_CAPACITY) {
            return -1;
        }
        return (int) entryId;
    }

    /**
     * Clears the BitSet bit for the given entryId (no-op if BitSet is disabled).
     */
    private static void clearBit(LedgerPendingAcks ledgerState, long entryId) {
        BitSet bits = ledgerState.entryBits;
        if (bits == null) {
            return;
        }
        int bitIndex = bitIndexOf(entryId);
        if (bitIndex >= 0) {
            bits.clear(bitIndex);
        }
    }

    private void handleRemovePendingAck(long ledgerId, long entryId, int stickyKeyHash) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        if (pendingAcksRemoveHandler != null) {
            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
        }
    }
}
