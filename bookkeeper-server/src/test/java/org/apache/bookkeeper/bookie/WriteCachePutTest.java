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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteCachePutTest {

    // Istanza della classe da testare
    private WriteCache writeCache;

    // Costanti per definire un ambiente di test consistente
    private static final long CACHE_CAPACITY = 1024;
    private static final int SEGMENT_SIZE = 1024;

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
}