package app.l2nx.gs.db.sync.engine.persist;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

/**
 * Filesystem-backed {@link SnapshotPersistence}. One file per entity under
 * {@code <schemaDir>/<entity>.snap}, written via tmp + atomic rename so a
 * crash mid-write leaves the previous good copy intact. A shared directory
 * lock ({@code <schemaDir>/.lock}) prevents two adapter JVMs from clobbering
 * each other on the same host.
 *
 * <p>Binary record layout (big-endian):</p>
 * <pre>
 * magic         : 4 bytes ASCII "NXSS"
 * version       : int16 = 1
 * entityNameLen : int16 (UTF-8 byte length)
 * entityName    : entityNameLen bytes (UTF-8)
 * count         : int32 entry count
 * entries       : count × (int64 pk, int32 crc) — 12 bytes each
 * bodyCrc32     : int32 CRC32 of count + entries (raw bytes)
 * </pre>
 *
 * <p>Throttling: per-entity {@link #checkpoint} writes are skipped when the
 * previous write for that entity <em>completed</em> less than
 * {@code checkpointMinIntervalSeconds} ago — slow fsyncs stretch the
 * effective window past the configured value, which is the intended
 * "don't double-fsync within N seconds of completion" semantics.
 * {@link #flushAll} bypasses the throttle so the freshest state always
 * survives shutdown.</p>
 */
public final class FileSnapshotPersistence implements SnapshotPersistence {

    private static final NxLog log = NxLogFactory.getLogger(FileSnapshotPersistence.class);

    static final byte[] MAGIC = new byte[] {'N', 'X', 'S', 'S'};
    static final short FORMAT_VERSION = 1;
    static final String SNAPSHOT_SUFFIX = ".snap";
    static final String TMP_SUFFIX = SNAPSHOT_SUFFIX + ".tmp";
    static final String LOCK_FILE = ".lock";
    static final int MAX_ENTITY_NAME_BYTES = 1024;
    static final int ENTRY_BYTES = 12;

    private final Path schemaDir;
    private final long minIntervalNanos;
    // Concurrent: different entities checkpoint from different CDC pool workers.
    // Single-writer-per-entity holds for SnapshotStore, NOT for this shared map.
    private final Map<String, Long> lastWriteNanos = new ConcurrentHashMap<String, Long>();

    private FileChannel lockChannel;
    private FileLock dirLock;

    public FileSnapshotPersistence(Path schemaDir, int checkpointMinIntervalSeconds) {
        if (checkpointMinIntervalSeconds < 0) {
            throw new IllegalArgumentException(
                    "checkpointMinIntervalSeconds must be >= 0, got " + checkpointMinIntervalSeconds);
        }
        this.schemaDir = schemaDir;
        this.minIntervalNanos = TimeUnit.SECONDS.toNanos(checkpointMinIntervalSeconds);
        acquireDirectoryLock();
    }

    private void acquireDirectoryLock() {
        try {
            Files.createDirectories(schemaDir);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to create snapshot directory '" + schemaDir + "': " + e.getMessage(), e);
        }
        Path lockPath = schemaDir.resolve(LOCK_FILE);
        FileChannel channel = null;
        try {
            channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock lock = channel.tryLock();
            if (lock == null) {
                throw new IllegalStateException("Another process already holds the snapshot lock '" + lockPath
                        + "' — refusing to start a second writer");
            }
            this.lockChannel = channel;
            this.dirLock = lock;
        } catch (OverlappingFileLockException e) {
            closeQuietly(channel);
            throw new IllegalStateException(
                    "Another thread in this JVM already holds the snapshot lock '" + lockPath
                            + "' — only one FileSnapshotPersistence per directory is allowed",
                    e);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new IllegalStateException(
                    "Unable to acquire snapshot directory lock '" + lockPath + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void load(SnapshotStore target) {
        ensureOpen();
        if (!Files.isDirectory(schemaDir)) {
            return;
        }
        int loadedEntities = 0;
        long loadedEntries = 0L;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(schemaDir, "*" + SNAPSHOT_SUFFIX)) {
            for (Path file : stream) {
                int count = loadOne(file, target);
                if (count >= 0) {
                    loadedEntities++;
                    loadedEntries += count;
                }
            }
        } catch (IOException e) {
            log.warn(
                    "FileSnapshotPersistence.load: directory scan of '{}' failed: {} — starting with empty snapshot",
                    schemaDir,
                    e.getMessage());
            return;
        }
        log.info(
                "FileSnapshotPersistence loaded {} entit{} ({} entries) from '{}'",
                loadedEntities,
                loadedEntities == 1 ? "y" : "ies",
                loadedEntries,
                schemaDir);
    }

    private int loadOne(Path file, SnapshotStore target) {
        final long fileSize;
        try {
            fileSize = Files.size(file);
        } catch (IOException e) {
            log.warn(
                    "FileSnapshotPersistence.load: '{}' size probe failed: {} — skipping",
                    file.getFileName(),
                    e.getMessage());
            return -1;
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file.toFile())))) {
            byte[] magic = new byte[MAGIC.length];
            in.readFully(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                log.warn("FileSnapshotPersistence.load: '{}' bad magic — skipping", file.getFileName());
                return -1;
            }
            short version = in.readShort();
            if (version != FORMAT_VERSION) {
                log.warn(
                        "FileSnapshotPersistence.load: '{}' unsupported version {} (expected {}) — skipping",
                        file.getFileName(),
                        version,
                        FORMAT_VERSION);
                return -1;
            }
            short nameLen = in.readShort();
            if (nameLen <= 0 || nameLen > MAX_ENTITY_NAME_BYTES) {
                log.warn(
                        "FileSnapshotPersistence.load: '{}' bad entityName length {} — skipping",
                        file.getFileName(),
                        nameLen);
                return -1;
            }
            byte[] nameBytes = new byte[nameLen];
            in.readFully(nameBytes);
            String entityName = new String(nameBytes, StandardCharsets.UTF_8);

            int count = in.readInt();
            if (count < 0) {
                log.warn("FileSnapshotPersistence.load: '{}' negative count {} — skipping", file.getFileName(), count);
                return -1;
            }
            // Cap count by what could fit in the file (entries plus trailing 4-byte
            // checksum). Without this a crafted header (e.g. count=Integer.MAX_VALUE)
            // triggers OOM in the Long2IntOpenHashMap pre-allocation BEFORE we get
            // to checksum-verify the truncated body.
            long maxFeasibleEntries = Math.max(0L, (fileSize - 4L) / ENTRY_BYTES);
            if ((long) count > maxFeasibleEntries) {
                log.warn(
                        "FileSnapshotPersistence.load: '{}' count {} exceeds file capacity ({}) — skipping",
                        file.getFileName(),
                        count,
                        maxFeasibleEntries);
                return -1;
            }
            CRC32 bodyHash = new CRC32();
            byte[] scratch = new byte[ENTRY_BYTES];
            ByteBuffer scratchView = ByteBuffer.wrap(scratch);

            scratchView.putInt(0, count);
            bodyHash.update(scratch, 0, 4);

            SnapshotStore.Loader loader = target.newLoader(entityName, count);
            for (int i = 0; i < count; i++) {
                in.readFully(scratch);
                bodyHash.update(scratch, 0, ENTRY_BYTES);
                long pk = scratchView.getLong(0);
                int crc = scratchView.getInt(8);
                loader.put(pk, crc);
            }
            int expectedChecksum = in.readInt();
            int actualChecksum = (int) bodyHash.getValue();
            if (expectedChecksum != actualChecksum) {
                log.warn(
                        "FileSnapshotPersistence.load: '{}' checksum mismatch (expected 0x{} got 0x{}) — skipping",
                        file.getFileName(),
                        Integer.toHexString(expectedChecksum),
                        Integer.toHexString(actualChecksum));
                return -1;
            }
            loader.commit();
            return count;
        } catch (EOFException eof) {
            log.warn("FileSnapshotPersistence.load: '{}' truncated — skipping", file.getFileName());
            return -1;
        } catch (IOException ioe) {
            log.warn(
                    "FileSnapshotPersistence.load: '{}' read error: {} — skipping",
                    file.getFileName(),
                    ioe.getMessage());
            return -1;
        } catch (RuntimeException re) {
            log.warn(
                    "FileSnapshotPersistence.load: '{}' decode error: {} — skipping",
                    file.getFileName(),
                    re.getMessage());
            return -1;
        }
    }

    @Override
    public void checkpoint(String entityName, SnapshotStore source) {
        ensureOpen();
        long now = System.nanoTime();
        Long last = lastWriteNanos.get(entityName);
        if (last != null && (now - last) < minIntervalNanos) {
            return;
        }
        if (writeOne(entityName, source)) {
            lastWriteNanos.put(entityName, System.nanoTime());
        }
    }

    @Override
    public void flushAll(SnapshotStore source) {
        ensureOpen();
        Set<String> entities = source.entityNames();
        int flushed = 0;
        for (String entity : entities) {
            if (writeOne(entity, source)) {
                lastWriteNanos.put(entity, System.nanoTime());
                flushed++;
            }
        }
        log.info(
                "FileSnapshotPersistence.flushAll: {} entit{} written to '{}'",
                flushed,
                flushed == 1 ? "y" : "ies",
                schemaDir);
    }

    private boolean writeOne(String entityName, SnapshotStore source) {
        Path target = schemaDir.resolve(entityName + SNAPSHOT_SUFFIX);
        Path tmp = schemaDir.resolve(entityName + TMP_SUFFIX);
        int sizeHint = source.sizeOf(entityName);
        try {
            try (FileOutputStream fos = new FileOutputStream(tmp.toFile());
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos))) {
                out.write(MAGIC);
                out.writeShort(FORMAT_VERSION);
                byte[] nameBytes = entityName.getBytes(StandardCharsets.UTF_8);
                if (nameBytes.length > MAX_ENTITY_NAME_BYTES) {
                    throw new IOException("entityName too long for format: " + nameBytes.length);
                }
                out.writeShort((short) nameBytes.length);
                out.write(nameBytes);

                final CRC32 bodyHash = new CRC32();
                final byte[] scratch = new byte[ENTRY_BYTES];
                final ByteBuffer scratchView = ByteBuffer.wrap(scratch);

                scratchView.putInt(0, sizeHint);
                out.write(scratch, 0, 4);
                bodyHash.update(scratch, 0, 4);

                final int[] writtenCount = new int[] {0};
                final IOException[] firstError = new IOException[1];

                source.forEachEntry(entityName, (pk, crc) -> {
                    if (firstError[0] != null) {
                        return;
                    }
                    scratchView.putLong(0, pk);
                    scratchView.putInt(8, crc);
                    try {
                        out.write(scratch, 0, ENTRY_BYTES);
                    } catch (IOException e) {
                        firstError[0] = e;
                        return;
                    }
                    bodyHash.update(scratch, 0, ENTRY_BYTES);
                    writtenCount[0]++;
                });
                if (firstError[0] != null) {
                    throw firstError[0];
                }
                if (writtenCount[0] != sizeHint) {
                    // Single-writer-per-entity contract guarantees agreement; a drift
                    // here means a future refactor broke the contract — fail loud
                    // before we commit a body whose count header lies.
                    throw new IOException(
                            "entry count drift during dump: expected " + sizeHint + " got " + writtenCount[0]);
                }
                out.writeInt((int) bodyHash.getValue());
                out.flush();
                fos.getFD().sync();
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException ioe) {
            log.warn(
                    "FileSnapshotPersistence.write({}): IO error: {} — leaving previous snapshot intact",
                    entityName,
                    ioe.getMessage());
        } catch (RuntimeException re) {
            log.warn(
                    "FileSnapshotPersistence.write({}): unexpected {} — leaving previous snapshot intact",
                    entityName,
                    re.getClass().getName(),
                    re);
        }
        deleteIfExists(tmp);
        return false;
    }

    @Override
    public void close() {
        if (dirLock == null) {
            return;
        }
        try {
            dirLock.release();
        } catch (IOException e) {
            log.warn("FileSnapshotPersistence.close: lock release failed: {}", e.getMessage());
        } finally {
            dirLock = null;
            closeQuietly(lockChannel);
            lockChannel = null;
        }
    }

    private void ensureOpen() {
        if (dirLock == null) {
            throw new IllegalStateException("FileSnapshotPersistence is closed");
        }
    }

    private static void deleteIfExists(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (NoSuchFileException ignored) {
            // raced — fine
        } catch (IOException e) {
            log.warn("FileSnapshotPersistence.deleteIfExists({}) failed: {}", p, e.getMessage());
        }
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("FileSnapshotPersistence: channel close failed: {}", e.getMessage());
        }
    }
}
