package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Suite di test JUnit 5 per la classe WriteCache.
 * Progettata per massimizzare la copertura di statement/conditional (JaCoCo)
 * e l'uccisione di mutanti (PIT).
 */
class LLMGuidedWriteCacheTest {

    private ByteBufAllocator allocator;
    private WriteCache writeCache;

    // Dimensioni scelte per facilitare i test sui limiti dei segmenti
    private static final int MAX_SEGMENT_SIZE = 128; // Potenza di 2 per validità
    private static final int NUM_SEGMENTS = 4;
    private static final long MAX_CACHE_SIZE = MAX_SEGMENT_SIZE * NUM_SEGMENTS; // 512 bytes

    @BeforeEach
    void setUp() {
        allocator = ByteBufAllocator.DEFAULT;
    }

    @AfterEach
    void tearDown() {
        if (writeCache != null) {
            writeCache.close();
        }
    }

    // --- Test del Costruttore ---

    /**
     * Esperto di Edge Cases: Verifica che il costruttore lanci IllegalArgumentException
     * per dimensioni di segmento non valide (0 o negative).
     * Uccide mutanti PIT come `maxSegmentSize > 0` -> `maxSegmentSize >= 0`.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, -1, -128})
    void testConstructor_InvalidSegmentSize_ZeroOrNegative(int invalidSegmentSize) {
        assertThrows(IllegalArgumentException.class, () -> {
            new WriteCache(allocator, MAX_CACHE_SIZE, invalidSegmentSize);
        }, "Il costruttore dovrebbe fallire per maxSegmentSize <= 0");
    }

    /**
     * Esperto di Edge Cases: Verifica che il costruttore lanci IllegalArgumentException
     * se la dimensione del segmento non è una potenza di due.
     * Uccide mutanti PIT che alterano il controllo `maxSegmentSize == alignedMaxSegmentSize`.
     */
    @ParameterizedTest
    @ValueSource(ints = {100, 127, 200})
    void testConstructor_InvalidSegmentSize_NotPowerOfTwo(int nonPowerOfTwoSize) {
        assertThrows(IllegalArgumentException.class, () -> {
            new WriteCache(allocator, MAX_CACHE_SIZE, nonPowerOfTwoSize);
        }, "Il costruttore dovrebbe fallire per maxSegmentSize che non è una potenza di 2");
    }

    /**
     * Esperto di Code Coverage: Assicura che il costruttore funzioni correttamente
     * quando maxCacheSize non è un multiplo esatto di maxSegmentSize,
     * coprendo la logica di calcolo dell'ultimo segmento.
     */
    @Test
    void testConstructor_CacheSizeNotMultipleOfSegmentSize() {
        long cacheSize = MAX_CACHE_SIZE + 64;
        writeCache = new WriteCache(allocator, cacheSize, MAX_SEGMENT_SIZE);
        assertNotNull(writeCache);
        // Il test implicito è che nessuna eccezione viene lanciata.
        // La logica interna di allocazione dei segmenti viene coperta.
    }

    // --- Test dei Metodi 'put' e 'get' ---

    /**
     * Tutti gli Esperti (Caso Standard): Testa il percorso felice (happy path).
     * Un'operazione base di put seguita da un get per verificare la correttezza dei dati.
     * Serve come base per l'uccisione di mutanti che alterano la logica fondamentale.
     */
    @Test
    void testPutAndGet_HappyPath() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
        byte[] data = "happy-path".getBytes();
        ByteBuf entry = Unpooled.wrappedBuffer(data);

        assertTrue(writeCache.put(1L, 1L, entry), "Il put dovrebbe avere successo");

        ByteBuf result = writeCache.get(1L, 1L);
        assertNotNull(result);

        byte[] retrievedData = new byte[result.readableBytes()];
        result.readBytes(retrievedData);

        assertArrayEquals(data, retrievedData, "I dati recuperati dovrebbero corrispondere a quelli inseriti");
        assertEquals(1, writeCache.count());
        assertEquals(data.length, writeCache.size());
        result.release();
    }

    /**
     * Esperto di Mutation Testing: Testa il caso in cui un entry non esista.
     * Uccide mutanti che cambiano `result == null` in `result != null`.
     * Assicura che il percorso di fallimento del get funzioni come previsto.
     */
    @Test
    void testGet_NotFound() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
        writeCache.put(1L, 1L, Unpooled.wrappedBuffer("data".getBytes()));

        assertNull(writeCache.get(1L, 2L), "Dovrebbe restituire null per entryId non esistente");
        assertNull(writeCache.get(2L, 1L), "Dovrebbe restituire null per ledgerId non esistente");
    }

    /**
     * Esperto di Code Coverage e Edge Cases: Testa lo scenario in cui la cache è piena.
     * Copre il ramo `(offset + size) > maxCacheSize`.
     * Uccide mutanti che cambiano `return false` in `return true` in caso di cache piena.
     */
    @Test
    void testPut_CacheFull() {
        // Usiamo una entry di 32 byte. La sua dimensione allineata a 64 è 64 byte.
        int entrySize = 32;
        ByteBuf entry = Unpooled.buffer(entrySize);
        entry.writeBytes(new byte[entrySize]);

        // MAX_CACHE_SIZE è 512. Spazio allocato per entry è 64. 512 / 64 = 8.
        // Possiamo inserire 8 entry prima di riempire lo spazio allocato.
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
        for (int i = 0; i < 8; i++) {
            assertTrue(writeCache.put(1L, i, entry.slice()), "Put " + i + " dovrebbe avere successo");
        }

        // CORREZIONE: La dimensione registrata è la somma delle dimensioni originali (8 * 32).
        long expectedSize = (long) entrySize * 8;
        assertEquals(expectedSize, writeCache.size(), "La dimensione dovrebbe essere la somma delle dimensioni originali delle entry");

        // Il prossimo put dovrebbe fallire perché lo spazio allocato è esaurito
        assertFalse(writeCache.put(1L, 8, entry.slice()), "Il put dovrebbe fallire perché la cache è piena");
        entry.release();
    }

    /**
     * Esperto di Code Coverage: Testa il caso in cui un'entry non entra nel segmento corrente
     * e deve passare a quello successivo. Copre il ramo `maxSegmentSize - localOffset < size`
     * e il `continue` nel ciclo.
     */
    @Test
    void testPut_EntrySpanningSegmentBoundary() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);

        // Inseriamo un'entry la cui dimensione allineata è 128 (es. 96 bytes).
        // Questo riempie il primo segmento in termini di offset.
        ByteBuf firstEntry = Unpooled.buffer(96);
        firstEntry.writeBytes(new byte[96]);
        assertTrue(writeCache.put(1L, 1L, firstEntry.slice()));

        // Ora il primo segmento (128 byte di spazio) è pieno. Il prossimo put deve andare nel secondo.
        ByteBuf secondEntry = Unpooled.buffer(32);
        secondEntry.writeBytes(new byte[32]);
        assertTrue(writeCache.put(2L, 1L, secondEntry.slice()), "Il secondo put dovrebbe avere successo nel segmento successivo");

        // Verifichiamo che entrambi gli entry siano recuperabili
        assertNotNull(writeCache.get(1L, 1L));
        assertNotNull(writeCache.get(2L, 1L));

        firstEntry.release();
        secondEntry.release();
    }

    /**
     * Esperto di Mutation Testing: Un test di precisione per il confine del segmento.
     * Inserisce un entry che occupa esattamente lo spazio rimanente in un segmento.
     * Uccide mutanti che cambiano `<` in `<=` nella condizione di spanning del segmento.
     */
    @Test
    void testPut_EntryExactlyFitsAtSegmentEnd() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);

        // Entry di 32 byte (dimensione allineata 64). Occupa la prima metà del segmento.
        ByteBuf entry1 = Unpooled.buffer(32);
        entry1.writeBytes(new byte[32]);
        assertTrue(writeCache.put(1L, 1L, entry1.slice()));

        // Ora rimangono 128 - 64 = 64 byte di spazio allocabile nel primo segmento.
        // Inseriamo un altro entry di 32 byte (dimensione allineata 64) che lo riempie.
        ByteBuf entry2 = Unpooled.buffer(32);
        entry2.writeBytes(new byte[32]);
        assertTrue(writeCache.put(1L, 2L, entry2.slice()), "Il secondo put dovrebbe riempire esattamente il segmento");

        // CORREZIONE: La dimensione registrata è la somma delle dimensioni reali (32 + 32).
        assertEquals(64, writeCache.size(), "La dimensione dovrebbe essere la somma delle dimensioni originali");
        assertNotNull(writeCache.get(1L, 1L));
        assertNotNull(writeCache.get(1L, 2L));

        entry1.release();
        entry2.release();
    }

    /**
     * Esperto di Edge Cases: Testa l'inserimento di un'entry vuota.
     * La dimensione allineata sarà 64, ma la dimensione effettiva è 0.
     * Verifica che il sistema gestisca correttamente lo stato.
     */
    @Test
    void testPutAndGet_EmptyEntry() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
        ByteBuf emptyEntry = Unpooled.EMPTY_BUFFER;

        assertTrue(writeCache.put(1L, 1L, emptyEntry), "Il put di un'entry vuota dovrebbe avere successo");
        assertEquals(1, writeCache.count());
        assertEquals(0, writeCache.size(), "La dimensione della cache non dovrebbe aumentare per un'entry vuota");

        ByteBuf result = writeCache.get(1L, 1L);
        assertNotNull(result);
        assertEquals(0, result.readableBytes(), "L'entry recuperata dovrebbe essere vuota");
        result.release();
    }

    /**
     * Esperto di Code Coverage e Mutation Testing: Testa la logica di aggiornamento
     * dell'ultimo entryId per un ledger.
     * Copre il ramo in cui un entryId più recente sostituisce quello vecchio.
     */
    @Test
    void testPut_UpdatesLastEntryId() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
        ByteBuf entry5 = Unpooled.wrappedBuffer("entry5".getBytes());
        ByteBuf entry10 = Unpooled.wrappedBuffer("entry10".getBytes());

        writeCache.put(1L, 5L, entry5);
        writeCache.put(1L, 10L, entry10); // entryId più recente

        ByteBuf lastEntry = writeCache.getLastEntry(1L);
        assertNotNull(lastEntry);
        byte[] retrievedData = new byte[lastEntry.readableBytes()];
        lastEntry.readBytes(retrievedData);

        assertArrayEquals("entry10".getBytes(), retrievedData, "Dovrebbe essere restituito l'entry con ID più alto");
        lastEntry.release();
    }

    /**
     * Esperto di Mutation Testing: Testa la logica di non-aggiornamento per un entryId obsoleto.
     * Copre il ramo `currentLastEntryId > entryId`.
     * Uccide mutanti che cambiano `>` in `>=` o che rimuovono questo controllo.
     */
    @Test
    void testPut_StaleEntryIdDoesNotUpdateLastEntry() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
        ByteBuf entry10 = Unpooled.wrappedBuffer("entry10".getBytes());
        ByteBuf entry5 = Unpooled.wrappedBuffer("entry5".getBytes());

        writeCache.put(1L, 10L, entry10);
        writeCache.put(1L, 5L, entry5); // entryId obsoleto

        ByteBuf lastEntry = writeCache.getLastEntry(1L);
        assertNotNull(lastEntry);
        byte[] retrievedData = new byte[lastEntry.readableBytes()];
        lastEntry.readBytes(retrievedData);

        assertArrayEquals("entry10".getBytes(), retrievedData, "L'entry obsoleto non dovrebbe sostituire quello più recente");
        lastEntry.release();
    }
}