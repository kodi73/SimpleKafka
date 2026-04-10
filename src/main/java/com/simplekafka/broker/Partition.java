package com.simplekafka.broker;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Partition represents a single partition of a Kafka topic.
 *
 * It manages:
 *  - Writing messages to segmented log files on disk
 *  - Reading messages back by offset
 *  - Maintaining index files for fast offset lookup
 *  - Thread-safe concurrent access via ReadWriteLock
 *
 * Each partition lives in its own directory and contains
 * multiple segment files (log + index pairs).
 */
public class Partition {

    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());

    // Maximum size of one log segment before we roll to a new one.
    // Real Kafka defaults to 1GB — we use 1MB for easy testing.
    private static final long SEGMENT_SIZE = 1024 * 1024; // 1 MB

    // Each index entry is exactly 16 bytes:
    // 8 bytes for the offset + 8 bytes for the file position
    private static final int INDEX_ENTRY_SIZE = 16;

    // ----------------------------------------------------------------
    // Core identity fields
    // ----------------------------------------------------------------

    private final int id;           // Partition number (0, 1, 2, ...)
    private int leader;             // Broker ID of the current leader
    private List<Integer> followers; // Broker IDs of follower replicas

    // ----------------------------------------------------------------
    // Storage fields
    // ----------------------------------------------------------------

    // Root directory for this partition's files
    // e.g., /tmp/simplekafka/broker-1/payments-0/
    private final String baseDir;

    // The next offset to assign to an incoming message.
    // AtomicLong because multiple threads might check it concurrently.
    private final AtomicLong nextOffset;

    // Allows multiple concurrent readers OR one exclusive writer
    private final ReadWriteLock lock;

    // The currently active (writable) log file handle
    private RandomAccessFile activeLogFile;

    // FileChannel wraps the RandomAccessFile for efficient NIO operations
    // FileChannel supports direct OS-level I/O — faster than stream-based I/O
    private FileChannel activeLogChannel;

    // Tracks all segments in this partition (in offset order)
    private final List<SegmentInfo> segments;

    // ----------------------------------------------------------------
    // Inner class: tracks one segment's metadata in memory
    // ----------------------------------------------------------------

    /**
     * SegmentInfo holds just enough info to locate and use a segment.
     * The actual data lives on disk — this is just the in-memory handle.
     */
    private static class SegmentInfo {
        // The offset of the first message in this segment.
        // This is also the number in the filename:
        // baseOffset=127 → "00000000000000000127.log"
        final long baseOffset;

        // Full path to the .log file for this segment
        final String logPath;

        // Full path to the .index file for this segment
        final String indexPath;

        // How many messages are in this segment (used to calculate nextOffset)
        long messageCount;

        SegmentInfo(long baseOffset, String logPath, String indexPath, long messageCount) {
            this.baseOffset   = baseOffset;
            this.logPath      = logPath;
            this.indexPath    = indexPath;
            this.messageCount = messageCount;
        }
    }

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id        = id;
        this.leader    = leader;
        this.followers = followers;
        this.baseDir   = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock       = new ReentrantReadWriteLock();
        this.segments   = new ArrayList<>();

        initialize();
    }

    // ----------------------------------------------------------------
    // Initialization — recover state from disk on startup
    // ----------------------------------------------------------------

    /**
     * Scan the partition directory for existing segment files and
     * reconstruct our in-memory state from them.
     *
     * This is critical for crash recovery — if the broker restarts,
     * it must pick up exactly where it left off.
     */
    private void initialize() {
        try {
            // Create the directory if this is a brand new partition
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
                LOGGER.info("Created partition directory: " + baseDir);
            }

            // Scan the directory for existing .log files.
            // Each .log file is one segment.
            File[] logFiles = dir.listFiles(
                (d, name) -> name.endsWith(".log")
            );

            if (logFiles != null && logFiles.length > 0) {
                // Parse the base offset from each filename and build SegmentInfo
                for (File logFile : logFiles) {
                    String fileName = logFile.getName();
                    // Filename format: "00000000000000000127.log"
                    // Strip the ".log" to get the offset string "00000000000000000127"
                    long baseOffset = Long.parseLong(
                        fileName.substring(0, fileName.length() - 4)
                    );

                    String logPath   = logFile.getAbsolutePath();
                    String indexPath = logPath.replace(".log", ".index");

                    // Count how many messages are already in this segment
                    // by reading through the log file (each msg starts with a 4-byte length)
                    long messageCount = countMessagesInSegment(logPath);

                    segments.add(new SegmentInfo(baseOffset, logPath, indexPath, messageCount));
                }

                // Sort segments by their base offset — oldest first
                segments.sort((a, b) -> Long.compare(a.baseOffset, b.baseOffset));

                // Calculate what the next offset should be:
                // last segment's base offset + number of messages in it
                SegmentInfo last = segments.get(segments.size() - 1);
                nextOffset.set(last.baseOffset + last.messageCount);

                LOGGER.info("Recovered " + segments.size() + " segments. Next offset: " + nextOffset.get());

                // Open the last segment for appending new messages
                openSegmentForAppend(last);

            } else {
                // No existing data — create the very first segment starting at offset 0
                LOGGER.info("No existing segments found. Creating first segment.");
                createNewSegment(0);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
            throw new RuntimeException("Partition initialization failed", e);
        }
    }

    /**
     * Count how many messages are stored in a log file.
     *
     * Each message is stored as: [4-byte length][N bytes of data]
     * So we read the 4-byte length, skip N bytes, repeat until EOF.
     */
    private long countMessagesInSegment(String logPath) {
        long count = 0;
        try (RandomAccessFile raf = new RandomAccessFile(logPath, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                int msgLength = raf.readInt(); // Read the 4-byte length prefix
                raf.skipBytes(msgLength);      // Skip past the message data
                count++;
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error counting messages in: " + logPath, e);
        }
        return count;
    }

    // ----------------------------------------------------------------
    // Segment lifecycle
    // ----------------------------------------------------------------

    /**
     * Create a brand new segment starting at the given base offset.
     *
     * The filename encodes the base offset so we can find the right
     * segment for any given offset just by looking at filenames.
     *
     * Format: 20-digit zero-padded number (same as real Kafka)
     * e.g., offset 127 → "00000000000000000127"
     */
    private void createNewSegment(long baseOffset) throws IOException {
        // Build the zero-padded filename
        String paddedOffset = String.format("%020d", baseOffset);
        String logPath   = baseDir + File.separator + paddedOffset + ".log";
        String indexPath = baseDir + File.separator + paddedOffset + ".index";

        // Create the actual files on disk (empty to start)
        new File(logPath).createNewFile();
        new File(indexPath).createNewFile();

        // Track this segment in memory
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath, 0);
        segments.add(segment);

        LOGGER.info("Created new segment at offset " + baseOffset + ": " + logPath);

        // Open it immediately for writing
        openSegmentForAppend(segment);
    }

    /**
     * Open a segment's files so we can append messages to it.
     *
     * We use RandomAccessFile in "rw" mode (read-write) and seek to
     * the end so all writes are appended — never overwriting old data.
     */
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close the previously active segment's file handles first
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        if (activeLogFile != null) {
            activeLogFile.close();
        }

        // Open the log file in read-write mode
        activeLogFile    = new RandomAccessFile(segment.logPath, "rw");
        activeLogChannel = activeLogFile.getChannel();

        // Seek to end — all appends go after existing data
        activeLogFile.seek(activeLogFile.length());

        LOGGER.info("Opened segment for append: " + segment.logPath
                    + " at position " + activeLogFile.length());
    }

    // ----------------------------------------------------------------
    // WRITE — Append a message
    // ----------------------------------------------------------------

    /**
     * Append a message to this partition and return its assigned offset.
     *
     * The offset is this message's permanent address — consumers use it
     * to request exactly this message later.
     *
     * Steps:
     *  1. Acquire write lock (blocks all readers while we write)
     *  2. Roll to a new segment if current one is full
     *  3. Write [4-byte length][message bytes] to the log file
     *  4. Force to disk (durability guarantee)
     *  5. Update the index file
     *  6. Increment the offset counter
     *  7. Return the assigned offset
     */
    public long append(byte[] message) throws IOException {
        // Acquire exclusive write lock
        lock.writeLock().lock();
        try {
            // Check if current segment has exceeded our size limit
            if (activeLogFile.length() >= SEGMENT_SIZE) {
                LOGGER.info("Segment full, rolling to new segment at offset " + nextOffset.get());
                createNewSegment(nextOffset.get());
            }

            // Record the byte position BEFORE we write —
            // this goes into the index so readers can seek directly here
            long filePosition = activeLogChannel.position();

            // Assign this message its offset
            long offset = nextOffset.get();

            // Build the on-disk format: [4-byte length prefix][message bytes]
            // The length prefix tells readers how many bytes to read
            ByteBuffer writeBuffer = ByteBuffer.allocate(4 + message.length);
            writeBuffer.putInt(message.length); // 4-byte length prefix
            writeBuffer.put(message);           // actual message bytes
            writeBuffer.flip();

            // Write to the log file via FileChannel
            while (writeBuffer.hasRemaining()) {
                activeLogChannel.write(writeBuffer);
            }

            // Force data to disk — without this, data might sit in OS buffer
            // and be lost if the machine crashes before the OS flushes it
            activeLogChannel.force(true);

            // Update the index: record offset → file position mapping
            updateIndex(offset, filePosition);

            // Increment the offset counter for the next message
            nextOffset.incrementAndGet();

            // Update the message count in the active segment's metadata
            segments.get(segments.size() - 1).messageCount++;

            LOGGER.info("Appended message at offset " + offset
                        + ", position " + filePosition);
            return offset;

        } finally {
            // ALWAYS release the lock, even if an exception occurred
            lock.writeLock().unlock();
        }
    }

    /**
     * Write an entry to the index file for the active segment.
     *
     * Index entry format: [8-byte offset][8-byte file position] = 16 bytes
     *
     * This lets us find any message's exact byte position in the log
     * file without scanning through all previous messages.
     */
    private void updateIndex(long offset, long position) throws IOException {
        SegmentInfo activeSegment = segments.get(segments.size() - 1);

        // Open the index file, seek to end, append the new entry
        try (RandomAccessFile indexFile = new RandomAccessFile(activeSegment.indexPath, "rw");
             FileChannel indexChannel  = indexFile.getChannel()) {

            indexChannel.position(indexChannel.size()); // Seek to end

            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            indexBuffer.putLong(offset);   // 8 bytes: the message's global offset
            indexBuffer.putLong(position); // 8 bytes: its byte position in the .log file
            indexBuffer.flip();

            indexChannel.write(indexBuffer);
            indexChannel.force(true); // Flush index to disk too
        }
    }

    // ----------------------------------------------------------------
    // READ — Fetch messages by offset
    // ----------------------------------------------------------------

    /**
     * Read messages starting from the given offset, up to maxBytes total.
     *
     * Steps:
     *  1. Acquire read lock (allows concurrent readers)
     *  2. Find which segment contains this offset
     *  3. Use the index to find the exact byte position in the log file
     *  4. Read messages sequentially until we hit maxBytes
     *  5. Return the list of raw message byte arrays
     */
    public List<byte[]> readMessages(long offset, int maxBytes) throws IOException {
        lock.readLock().lock();
        try {
            List<byte[]> messages = new ArrayList<>();

            // Validate the offset is within range
            if (offset < 0 || offset >= nextOffset.get()) {
                LOGGER.info("Requested offset " + offset
                            + " is out of range (next offset: " + nextOffset.get() + ")");
                return messages; // Return empty list — no messages at this offset
            }

            // Find the segment that contains this offset
            SegmentInfo segment = findSegmentForOffset(offset);
            if (segment == null) {
                LOGGER.warning("No segment found for offset " + offset);
                return messages;
            }

            // Use the index to find the exact byte position in the log file
            long filePosition = findPositionForOffset(segment, offset);

            // Now read messages from that position until we've read maxBytes
            int totalBytesRead = 0;
            try (RandomAccessFile logFile    = new RandomAccessFile(segment.logPath, "r");
                 FileChannel      logChannel = logFile.getChannel()) {

                logChannel.position(filePosition); // Jump directly to where we need to start

                while (totalBytesRead < maxBytes) {
                    // Try to read the 4-byte length prefix
                    ByteBuffer lenBuffer = ByteBuffer.allocate(4);
                    int bytesRead = logChannel.read(lenBuffer);

                    if (bytesRead < 4) {
                        // We've reached the end of this segment
                        // In a full implementation we'd continue to the next segment
                        break;
                    }

                    lenBuffer.flip();
                    int msgLength = lenBuffer.getInt();

                    // Check we won't exceed maxBytes
                    if (totalBytesRead + msgLength > maxBytes) {
                        break;
                    }

                    // Read the actual message bytes
                    ByteBuffer msgBuffer = ByteBuffer.allocate(msgLength);
                    logChannel.read(msgBuffer);
                    msgBuffer.flip();

                    byte[] msgBytes = new byte[msgLength];
                    msgBuffer.get(msgBytes);
                    messages.add(msgBytes);

                    totalBytesRead += 4 + msgLength; // Count length prefix + message
                }
            }

            LOGGER.info("Read " + messages.size() + " messages from offset " + offset);
            return messages;

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Find which segment contains a given offset.
     *
     * Each segment covers offsets from its baseOffset up to
     * (next segment's baseOffset - 1). The last segment covers
     * from its baseOffset to infinity (nextOffset - 1).
     *
     * We scan from newest to oldest since recent reads are more common.
     */
    private SegmentInfo findSegmentForOffset(long offset) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            SegmentInfo segment = segments.get(i);
            if (offset >= segment.baseOffset) {
                return segment;
            }
        }
        return null;
    }

    /**
     * Use the index file to find the byte position of a given offset
     * within its segment's log file.
     *
     * The index stores entries as [8-byte offset][8-byte position].
     * We calculate which entry to read directly from the relative offset.
     *
     * "Relative offset" = global offset - segment's base offset
     * e.g., global offset 135 in segment starting at 127 → relative offset 8
     * That means it's the 9th message in this segment (0-indexed).
     * Index entry for it is at byte position: 8 * INDEX_ENTRY_SIZE
     */
    private long findPositionForOffset(SegmentInfo segment, long offset) {
        long relativeOffset = offset - segment.baseOffset;

        try (RandomAccessFile indexFile    = new RandomAccessFile(segment.indexPath, "r");
             FileChannel       indexChannel = indexFile.getChannel()) {

            // Each index entry is exactly INDEX_ENTRY_SIZE (16) bytes
            // So entry N starts at byte N * 16
            long indexPosition = relativeOffset * INDEX_ENTRY_SIZE;

            if (indexPosition >= indexChannel.size()) {
                // Offset is beyond what we've indexed — start from beginning of segment
                return 0;
            }

            indexChannel.position(indexPosition);

            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            indexChannel.read(indexBuffer);
            indexBuffer.flip();

            indexBuffer.getLong(); // Read (and discard) the offset — we already know it
            return indexBuffer.getLong(); // Read the file position — this is what we want

        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Could not read index for offset " + offset
                        + " in segment " + segment.logPath, e);
            return 0; // Fall back to scanning from start of segment
        }
    }

    // ----------------------------------------------------------------
    // Getters and setters
    // ----------------------------------------------------------------

    public int getId()               { return id; }
    public int getLeader()           { return leader; }
    public void setLeader(int leader){ this.leader = leader; }
    public List<Integer> getFollowers()                    { return followers; }
    public void setFollowers(List<Integer> followers)      { this.followers = followers; }
    public long getNextOffset()      { return nextOffset.get(); }

    /**
     * Clean up file handles when the broker shuts down.
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }
            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}