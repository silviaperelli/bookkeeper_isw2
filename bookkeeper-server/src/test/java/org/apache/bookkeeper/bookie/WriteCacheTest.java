package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteCacheTest {

    // Tipi di Allocator per i test
    private enum AllocatorType {
        VALID,
        NULL,
        INVALID;

        public ByteBufAllocator getInstance() {
            switch (this) {
                case VALID:
                    return UnpooledByteBufAllocator.DEFAULT;
                case NULL:
                    return null;
                case INVALID:
                    ByteBufAllocator invalidAllocator = mock(ByteBufAllocator.class);
                    when(invalidAllocator.buffer(anyInt(), anyInt())).thenReturn(null);
                    return invalidAllocator;
                default:
                    return null;
            }
        }
    }

    // Tipi di dimensioni della Cache per i test
    private enum CacheSizeType {
        NEGATIVE(-1L),
        ZERO(0L),
        SMALL(1L),
        OFF_BY_ONE(1025L),
        STANDARD(1024L);

        private final long size;
        CacheSizeType(long size) { this.size = size; }
        public long getSize() { return size; }
    }

    // Tipi di dimensioni dei Segmenti per i test
    private enum SegmentSizeType {
        NEGATIVE(-1),
        ZERO(0),
        SMALL(1),
        NOT_POWER_OF_TWO(1023),
        STANDARD(1024),
        SMALLER_THAN_CACHE(512),
        LARGER_THAN_CACHE(2048);

        private final int size;
        SegmentSizeType(int size) { this.size = size; }
        public int getSize() { return size; }
    }

    // Metodo che fornisce i dati per il test parametrico
    private static Stream<Arguments> data() {
        return Stream.of(
                // Casi che devono lanciare un'eccezione durante la costruzione
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.NEGATIVE.getSize(), SegmentSizeType.SMALL.getSize(), ArrayIndexOutOfBoundsException.class),
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.ZERO.getSize(),     SegmentSizeType.ZERO.getSize(),     IllegalArgumentException.class),
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.STANDARD.getSize(), SegmentSizeType.NEGATIVE.getSize(), IllegalArgumentException.class),
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.STANDARD.getSize(), SegmentSizeType.NOT_POWER_OF_TWO.getSize(), IllegalArgumentException.class),
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.SMALL.getSize(), SegmentSizeType.ZERO.getSize(), IllegalArgumentException.class),

                // Casi che devono passare la costruzione (anche se l'allocatore è invalido o nullo)
                Arguments.of(AllocatorType.NULL.getInstance(),    CacheSizeType.STANDARD.getSize(), SegmentSizeType.STANDARD.getSize(), null),
                Arguments.of(AllocatorType.INVALID.getInstance(), CacheSizeType.STANDARD.getSize(), SegmentSizeType.STANDARD.getSize(), null), // Questo test passerà la costruzione
                Arguments.of(AllocatorType.VALID.getInstance(),   CacheSizeType.ZERO.getSize(),     SegmentSizeType.SMALL.getSize(), null),
                Arguments.of(AllocatorType.VALID.getInstance(),   CacheSizeType.SMALL.getSize(),    SegmentSizeType.SMALL.getSize(), null),
                Arguments.of(AllocatorType.VALID.getInstance(),   CacheSizeType.STANDARD.getSize(),    SegmentSizeType.STANDARD.getSize(), null),
                Arguments.of(AllocatorType.VALID.getInstance(),   CacheSizeType.STANDARD.getSize(), SegmentSizeType.LARGER_THAN_CACHE.getSize(), null),
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.STANDARD.getSize(), SegmentSizeType.SMALLER_THAN_CACHE.getSize(), null),
                Arguments.of(AllocatorType.VALID.getInstance(), CacheSizeType.OFF_BY_ONE.getSize(), SegmentSizeType.STANDARD.getSize(), null)
        );
    }

    @ParameterizedTest
    @MethodSource("data")
    void testWriteCacheCreation(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize, Class<? extends Throwable> expectedException) {
        if (expectedException != null) {
            assertThrows(expectedException, () -> new WriteCache(allocator, maxCacheSize, maxSegmentSize));
        } else {
            // Se non ci aspettiamo eccezioni, verifica che la costruzione abbia successo
            // e che lo stato iniziale sia corretto
            WriteCache writeCache = assertDoesNotThrow(() -> new WriteCache(allocator, maxCacheSize, maxSegmentSize));

            assertNotNull(writeCache);
            assertEquals(0, writeCache.size(), "La dimensione iniziale dovrebbe essere 0");
            assertEquals(0, writeCache.count(), "Il conteggio iniziale dovrebbe essere 0");
            assertTrue(writeCache.isEmpty(), "La cache dovrebbe essere vuota all'inizio");
        }
    }

}