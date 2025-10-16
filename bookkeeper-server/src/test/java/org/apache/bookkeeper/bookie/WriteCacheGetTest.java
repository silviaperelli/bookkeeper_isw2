package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class WriteCacheGetTest {

    // Istanza della classe da testare
    private WriteCache writeCache;

    // Costanti per definire un ambiente di test consistente
    private static final long CACHE_CAPACITY = 1024;
    private static final int SEGMENT_SIZE = 512;

    // Enumerazione per descrivere il contesto del test, indica se la chiave è presente o meno nella cache
    private enum TestContext {
        KEY_PRESENT,
        KEY_NOT_PRESENT
    }

    @BeforeEach
    void setUp() {
        // Inizializza una nuova cache prima di ogni test
        writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, CACHE_CAPACITY, SEGMENT_SIZE);
    }

    @AfterEach
    void tearDown() {
        // Rilascia le risorse della cache
        if (writeCache != null) {
            writeCache.close();
        }
    }

    private static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(-1L, 1L, TestContext.KEY_NOT_PRESENT, null, IllegalArgumentException.class),
                Arguments.of(1L, -1L, TestContext.KEY_NOT_PRESENT, null, null),
                Arguments.of(1L, 1L, TestContext.KEY_PRESENT, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}), null),
                Arguments.of(1L, 1L, TestContext.KEY_NOT_PRESENT, null, null)
        );
    }

    @ParameterizedTest
    @MethodSource("data")
    void testGet(long ledgerId, long entryId, TestContext context, ByteBuf expectedEntry, Class<? extends Throwable> expectedException) {
        // Gestione dei casi di fallimento
        if (expectedException != null) {
            assertThrows(expectedException, () -> writeCache.get(ledgerId, entryId));
            if (expectedEntry != null) {expectedEntry.release();}
        } else {
            // Gestione dei casi di successo
            ByteBuf entryToInsert = null;
            ByteBuf actualEntry = null;

            try {
                // Popola la cache
                if (context == TestContext.KEY_PRESENT) {
                    entryToInsert = expectedEntry.retainedDuplicate();
                    assertTrue(writeCache.put(ledgerId, entryId, entryToInsert), "Il setup del test (put) è fallito.");
                }

                actualEntry = writeCache.get(ledgerId, entryId);

                // Verifica dei test
                if (context == TestContext.KEY_PRESENT) {
                    assertNotNull(actualEntry, "L'entry recuperata non dovrebbe essere nulla.");
                    assertEquals(expectedEntry, actualEntry, "L'entry recuperata non corrisponde a quella inserita.");
                } else {
                    assertNull(actualEntry, "L'entry recuperata dovrebbe essere nulla.");
                }
            } finally {
                // Pulizia delle risorse allocate
                if (expectedEntry != null) {
                    expectedEntry.release();
                }
                if (entryToInsert != null) {
                    entryToInsert.release();
                }
                if (actualEntry != null) {
                    actualEntry.release();
                }
            }
        }
    }
}
