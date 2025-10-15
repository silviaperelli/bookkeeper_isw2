package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteCachePutTest {

    // Istanza della classe da testare
    private WriteCache writeCache;

    // Costanti per definire un ambiente di test consistente
    private static final long CACHE_CAPACITY = 1024;
    private static final int SEGMENT_SIZE = 512; // Segmenti più piccoli per testare il salto

    // Variabili per tracciare lo stato della cache prima dell'esecuzione del metodo
    private long sizeBefore;
    private long countBefore;

    // Tipi di ID (ledgerId e entryId) da testare
    private enum IdType {
        POSITIVE(1L),
        ZERO(0L),
        NEGATIVE(-1L);

        private final long id;
        IdType(long id) { this.id = id; }
        public long getId() { return id; }
    }

    // Tipi di ByteBuf per i test
    private enum EntryType {
        VALID_SMALL,
        VALID_LARGE,
        ZERO_SIZE,
        TOO_LARGE,
        NULL,
        INVALID_RELEASED;

        public ByteBuf getInstance() {
            switch (this) {
                case VALID_SMALL:
                    return Unpooled.wrappedBuffer(new byte[10]);
                case VALID_LARGE:
                    return Unpooled.wrappedBuffer(new byte[100]);
                case ZERO_SIZE:
                    return Unpooled.wrappedBuffer(new byte[0]);
                case TOO_LARGE:
                    return Unpooled.wrappedBuffer(new byte[(int) CACHE_CAPACITY + 1]);
                case NULL:
                    return null;
                case INVALID_RELEASED:
                    // Usa Mockito per simulare un ByteBuf in uno stato invalido
                    ByteBuf mockBuf = mock(ByteBuf.class);
                    when(mockBuf.readableBytes()).thenReturn(2);
                    when(mockBuf.readerIndex()).thenReturn(-1);
                    return mockBuf;
                default:
                    return null;
            }
        }
    }

    @BeforeEach
    void setUp() {
        // Crea una cache pulita prima di ogni esecuzione
        writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, CACHE_CAPACITY, SEGMENT_SIZE);
        // Salva lo stato iniziale (0) per le verifiche successive
        sizeBefore = writeCache.size();
        countBefore = writeCache.count();
    }

    @AfterEach
    void tearDown() {
        // Pulisce la memoria dopo ogni test per evitare memory leak
        if (writeCache != null) {
            writeCache.close();
        }
    }

    private static Stream<Arguments> providePutArguments() {
        return Stream.of(
                Arguments.of(IdType.NEGATIVE.getId(), IdType.POSITIVE.getId(), EntryType.VALID_SMALL.getInstance(), false, IllegalArgumentException.class),
                Arguments.of(IdType.POSITIVE.getId(), IdType.NEGATIVE.getId(), EntryType.VALID_SMALL.getInstance(), false, IllegalArgumentException.class),
                Arguments.of(IdType.ZERO.getId(),     IdType.ZERO.getId(),     EntryType.VALID_LARGE.getInstance(), true, null),
                Arguments.of(IdType.ZERO.getId(),     IdType.ZERO.getId(),     EntryType.TOO_LARGE.getInstance(),  false, null),
                Arguments.of(IdType.ZERO.getId(),     IdType.ZERO.getId(),     EntryType.ZERO_SIZE.getInstance(), true, null),
                Arguments.of(IdType.POSITIVE.getId(), IdType.POSITIVE.getId(), EntryType.NULL.getInstance(),         false, NullPointerException.class),
                Arguments.of(IdType.POSITIVE.getId(), IdType.POSITIVE.getId(), EntryType.INVALID_RELEASED.getInstance(), false, IndexOutOfBoundsException.class)
        );
    }

    @ParameterizedTest
    @MethodSource("providePutArguments")
    void testPut(long ledgerId, long entryId, ByteBuf entry, boolean expectedResult, Class<? extends Throwable> expectedException) {
        // Gestione dei casi di fallimento
        if (expectedException != null) {
            assertThrows(expectedException, () -> writeCache.put(ledgerId, entryId, entry));
            // Verifica che lo stato della cache non sia cambiato dopo il fallimento
            assertEquals(sizeBefore, writeCache.size());
            assertEquals(countBefore, writeCache.count());
        } else {
            // Gestione dei casi di successo
            boolean actualResult = assertDoesNotThrow(() -> writeCache.put(ledgerId, entryId, entry));
            assertEquals(expectedResult, actualResult, "Il risultato booleano del metodo put() non è quello atteso.");

            if (expectedResult) {
                // Verifica che la dimensione della cache sia aumentata e che il numero delle entry sia aumentato di 1
                assertEquals(sizeBefore + entry.readableBytes(), writeCache.size(), "La dimensione della cache non è aumentata correttamente.");
                assertEquals(countBefore + 1, writeCache.count(), "Il contatore delle entry non è aumentato di 1.");
                // Verifica che l'entry inserita corrisponde a quella iniziale
                ByteBuf retrievedEntry = writeCache.get(ledgerId, entryId);
                assertNotNull(retrievedEntry, "L'entry recuperata non dovrebbe essere nulla.");
                assertEquals(entry, retrievedEntry, "L'entry recuperata non corrisponde a quella inserita.");
                if (retrievedEntry != null) retrievedEntry.release();
            } else {
                assertEquals(sizeBefore, writeCache.size(), "La dimensione della cache non doveva cambiare.");
                assertEquals(countBefore, writeCache.count(), "Il contatore delle entry non doveva cambiare.");
            }
        }
    }


    // Test aggiuntivi dopo Jacoco
    @Test
    void testPutTriggersContinueAcrossSegmentBoundary() {
        // La dimensione della prima entry, allineata a 64 byte, non rimepie completamente il primo segmento
        int firstEntrySize = 400;
        ByteBuf firstEntry = Unpooled.buffer(firstEntrySize).writerIndex(firstEntrySize);
        assertTrue(writeCache.put(1L, 1L, firstEntry), "Il primo inserimento doveva avere successo.");

        // La seconda entry ha una dimensione maggiore dello spazio rimanente
        int secondEntrySize = 100;
        ByteBuf secondEntry = Unpooled.buffer(secondEntrySize).writerIndex(secondEntrySize);

        // Eseguendo la put del secondo segmento, si salterà al segmento successivo
        boolean result = writeCache.put(2L, 1L, secondEntry);

        // Verifica che il secondo inserimento sia riuscito e che entrambe le query siano presenti
        assertTrue(result, "Il secondo inserimento (che attiva il continue) doveva avere successo.");
        assertNotNull(writeCache.get(1L, 1L), "La prima entry dovrebbe essere recuperabile.");
        assertNotNull(writeCache.get(2L, 1L), "La seconda entry dovrebbe essere recuperabile.");
        assertEquals(2, writeCache.count(), "Dovrebbero esserci due entry nella cache.");
        assertEquals(firstEntrySize + secondEntrySize, writeCache.size(), "La dimensione totale della cache deve corrispondere alla somma delle entry.");
    }

    @Test
    void testPutWithOutOfOrderEntries() {
        ByteBuf lastEntry = null;
        ByteBuf entry5Retrieved = null;
        ByteBuf entry3Retrieved = null;
        try {
            // Crea due entry distinte
            ByteBuf entry5 = Unpooled.buffer(10).writerIndex(10);
            ByteBuf entry3 = Unpooled.buffer(10).writerIndex(10);

            // Inserimento prima dell'entry con un ID più alto
            assertTrue(writeCache.put(1L, 5L, entry5));
            assertTrue(writeCache.put(1L, 3L, entry3));

            // Verifica che "getLastEntry" restituisce l'entry con ID più alto
            lastEntry = writeCache.getLastEntry(1L);
            entry5Retrieved = writeCache.get(1L, 5L);
            entry3Retrieved = writeCache.get(1L, 3L);

            // Verifica che entrambe le entry siano inserite e recuperabili
            assertEquals(entry5, lastEntry);
            assertEquals(entry5, entry5Retrieved);
            assertEquals(entry3, entry3Retrieved);
        } finally {
            // Rilascia tutti i buffer che hai recuperato
            if (lastEntry != null) lastEntry.release();
            if (entry5Retrieved != null) entry5Retrieved.release();
            if (entry3Retrieved != null) entry3Retrieved.release();
        }
    }
}