package app.l2nx.gs.db.sync.engine.persist;

import app.l2nx.gs.db.sync.engine.SnapshotStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FileSnapshotPersistenceTest {

    @Test
    void flushAll_thenLoad_shouldRoundTripAllEntities(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 0xCAFEBABE);
        src.putCrc("clan", 2L, 0xDEADBEEF);
        src.putCrc("character", 99L, 12345);

        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }

        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }

        assertEquals(0xCAFEBABE, dst.getCrc("clan", 1L));
        assertEquals(0xDEADBEEF, dst.getCrc("clan", 2L));
        assertEquals(12345, dst.getCrc("character", 99L));
        assertEquals(2, dst.sizeOf("clan"));
        assertEquals(1, dst.sizeOf("character"));
    }

    @Test
    void load_shouldYieldEmptyStore_whenDirectoryEmpty(@TempDir Path dir) {
        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        assertTrue(dst.entityNames().isEmpty());
    }

    @Test
    void load_shouldSkipFile_whenMagicCorrupted(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 100);
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }
        // Corrupt the magic header of the only entity file.
        Path clanFile = dir.resolve("clan.snap");
        try (RandomAccessFile raf = new RandomAccessFile(clanFile.toFile(), "rw")) {
            raf.seek(0L);
            raf.write(new byte[]{'X', 'X', 'X', 'X'});
        }

        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        assertTrue(dst.entityNames().isEmpty(), "corrupt magic must be ignored, not crash");
    }

    @Test
    void load_shouldSkipFile_whenChecksumMismatch(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 42L, 0xABCDEF01);
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }
        // Flip one bit somewhere in the body (past header, before trailing checksum).
        Path clanFile = dir.resolve("clan.snap");
        long fileLen = Files.size(clanFile);
        try (RandomAccessFile raf = new RandomAccessFile(clanFile.toFile(), "rw")) {
            // Sit somewhere in the entry-bytes region (header is ~14 bytes, trailing checksum 4).
            long target = fileLen - 8L;
            raf.seek(target);
            int b = raf.read();
            raf.seek(target);
            raf.write(b ^ 0x01);
        }

        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        assertTrue(dst.entityNames().isEmpty(), "tampered body must fail checksum and be skipped");
    }

    @Test
    void load_shouldSkipFile_whenCountClaimsMoreThanFileCanHold(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 100);
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }
        // Header layout: 4-byte magic + 2-byte version + 2-byte nameLen + nameLen bytes name + 4-byte count.
        // For entityName "clan" (4 UTF-8 bytes), count offset = 4 + 2 + 2 + 4 = 12.
        Path clanFile = dir.resolve("clan.snap");
        try (RandomAccessFile raf = new RandomAccessFile(clanFile.toFile(), "rw")) {
            raf.seek(12L);
            raf.writeInt(Integer.MAX_VALUE);
        }

        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        assertTrue(dst.entityNames().isEmpty(),
                "absurd count header must be rejected before OOM-prone allocation");
    }

    @Test
    void load_shouldSkipFile_whenTruncated(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 1);
        src.putCrc("clan", 2L, 2);
        src.putCrc("clan", 3L, 3);
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }
        Path clanFile = dir.resolve("clan.snap");
        long fileLen = Files.size(clanFile);
        try (RandomAccessFile raf = new RandomAccessFile(clanFile.toFile(), "rw")) {
            raf.setLength(fileLen - 10L);
        }

        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        assertTrue(dst.entityNames().isEmpty());
    }

    @Test
    void checkpoint_shouldHonorThrottle_andFlushAllBypassesIt(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 100);

        // Throttle = 1 day — second checkpoint within the day must be a no-op.
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 24 * 3600)) {
            p.checkpoint("clan", src);
            long firstMtime = Files.getLastModifiedTime(dir.resolve("clan.snap")).toMillis();

            // Mutate, then call checkpoint again — file must NOT update.
            src.putCrc("clan", 2L, 200);
            Thread.sleep(50L); // ensure mtime tick would be observable if we wrote
            p.checkpoint("clan", src);
            long secondMtime = Files.getLastModifiedTime(dir.resolve("clan.snap")).toMillis();
            assertEquals(firstMtime, secondMtime, "throttled checkpoint must not rewrite the file");

            // flushAll ignores the throttle.
            Thread.sleep(50L);
            p.flushAll(src);
            long thirdMtime = Files.getLastModifiedTime(dir.resolve("clan.snap")).toMillis();
            assertTrue(thirdMtime > secondMtime, "flushAll must write regardless of throttle");
        }

        // Reload and check that flushAll-written content includes the second PK.
        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        assertEquals(2, dst.sizeOf("clan"));
    }

    @Test
    void constructor_shouldRefuseSecondInstanceOnSameDirectory(@TempDir Path dir) {
        FileSnapshotPersistence first = new FileSnapshotPersistence(dir, 0);
        try {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> new FileSnapshotPersistence(dir, 0));
            assertTrue(ex.getMessage().contains("lock"),
                    "expected lock-related error, got: " + ex.getMessage());
        } finally {
            first.close();
        }
        // After releasing the first, a fresh instance must succeed.
        try (FileSnapshotPersistence reborn = new FileSnapshotPersistence(dir, 0)) {
            assertNotNull(reborn);
        }
    }

    @Test
    void close_shouldBeIdempotent(@TempDir Path dir) {
        FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0);
        p.close();
        p.close();
        // After a close, ops must throw — invariant guarded by ensureOpen.
        assertThrows(IllegalStateException.class, () -> p.load(new SnapshotStore()));
    }

    @Test
    void writeOne_shouldLeavePreviousSnapshotIntact_whenWriteCrashesMidway(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 1);
        src.putCrc("clan", 2L, 2);
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }
        byte[] firstFile = Files.readAllBytes(dir.resolve("clan.snap"));

        // Force FileOutputStream(tmp) to fail by parking a directory at the tmp path.
        Path tmp = dir.resolve("clan.snap.tmp");
        Files.createDirectory(tmp);

        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            src.putCrc("clan", 3L, 3);
            p.flushAll(src);
        }

        byte[] afterFailure = Files.readAllBytes(dir.resolve("clan.snap"));
        assertArrayEquals(firstFile, afterFailure,
                "failed write must NOT clobber the previous good snapshot");
    }

    @Test
    void entitiesIsolated_eachInOwnFile(@TempDir Path dir) throws Exception {
        SnapshotStore src = new SnapshotStore();
        src.putCrc("clan", 1L, 10);
        src.putCrc("character", 2L, 20);
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.flushAll(src);
        }
        assertTrue(Files.exists(dir.resolve("clan.snap")));
        assertTrue(Files.exists(dir.resolve("character.snap")));

        SnapshotStore dst = new SnapshotStore();
        try (FileSnapshotPersistence p = new FileSnapshotPersistence(dir, 0)) {
            p.load(dst);
        }
        Set<String> names = dst.entityNames();
        assertTrue(names.contains("clan"));
        assertTrue(names.contains("character"));
    }

}
