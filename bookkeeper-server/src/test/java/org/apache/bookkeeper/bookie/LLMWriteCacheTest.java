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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite per la classe WriteCache.
 * Questa suite si concentra sulla verifica del costruttore, e dei metodi put() e get(),
 * con l'obiettivo di ottenere una copertura completa e di eliminare i mutanti generati da PIT.
 */
class LLMWriteCacheTest {

    private ByteBufAllocator allocator;
    private WriteCache cache;

    // Inizializza l'allocatore di memoria prima di ogni test
    @BeforeEach
    void setUp() {
        allocator = ByteBufAllocator.DEFAULT;
    }

    // Rilascia le risorse della cache dopo ogni test
    @AfterEach
    void tearDown() {
        if (cache != null) {
            cache.close();
        }
    }

    // === TEST SUL COSTRUTTORE ===

    /**
     * Testa la creazione di una WriteCache con parametri validi.
     * Si aspetta che l'istanza venga creata con successo.
     */
    @Test
    void testConstructorWithValidArguments() {
        cache = new WriteCache(allocator, 1024, 256);
        assertNotNull(cache);
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
    }

    /**
     * Testa il costruttore a due argomenti che usa un valore di default
     * per la dimensione massima del segmento.
     */
    @Test
    void testConstructorWithDefaultSegmentSize() {
        cache = new WriteCache(allocator, 1024 * 1024); // 1MB
        assertNotNull(cache);
    }


    /**
     * Verifica che il costruttore lanci un'eccezione se la dimensione massima
     * del segmento non è una potenza di due.
     */
    @Test
    void testConstructorWithNonPowerOfTwoSegmentSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            cache = new WriteCache(allocator, 1024, 300);
        }, "Max segment size needs to be in form of 2^n");
    }

    /**
     * Verifica che il costruttore lanci un'eccezione se la dimensione massima
     * del segmento è minore o uguale a zero.
     */
    @Test
    void testConstructorWithInvalidSegmentSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            cache = new WriteCache(allocator, 1024, 0);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            cache = new WriteCache(allocator, 1024, -1);
        });
    }


    // === TEST SUI METODI PUT E GET ===
    /**
     * Test base: inserisce una entry e la recupera con successo.
     */
    @Test
    void testPutAndGetSingleEntry() {
        cache = new WriteCache(allocator, 1024, 256);
        byte[] data = "test-data".getBytes();
        ByteBuf entry = Unpooled.wrappedBuffer(data);

        assertTrue(cache.put(1L, 1L, entry));

        ByteBuf retrievedEntry = cache.get(1L, 1L);
        assertNotNull(retrievedEntry);

        byte[] retrievedData = new byte[retrievedEntry.readableBytes()];
        retrievedEntry.readBytes(retrievedData);

        assertArrayEquals(data, retrievedData);
        assertEquals(1, cache.count());
        assertEquals(data.length, cache.size());
        assertFalse(cache.isEmpty());

        retrievedEntry.release();
    }

    /**
     * Verifica che il metodo get() restituisca null se la entry non esiste.
     */
    @Test
    void testGetNonExistentEntry() {
        cache = new WriteCache(allocator, 1024, 256);
        assertNull(cache.get(1L, 1L));
    }

    /**
     * Testa l'inserimento di una entry quando la cache è piena.
     * Il metodo put() dovrebbe restituire `false`.
     */
    @Test
    void testPutEntryWhenCacheIsFull() {
        // La dimensione del segmento DEVE essere una potenza di due. Usiamo 128.
        // La dimensione totale della cache può essere qualsiasi valore. La impostiamo a 100,
        // che è abbastanza per la prima entry (60 byte) ma non per la seconda.
        cache = new WriteCache(allocator, 100, 128);

        byte[] data = new byte[60]; // Dimensione reale 60, dimensione allineata 64
        ByteBuf entry = Unpooled.wrappedBuffer(data);

        // Il primo inserimento va a buon fine.
        // Controllo: (offset 0 + size 60) > 100  -->  60 > 100 è FALSO. L'inserimento procede.
        assertTrue(cache.put(1L, 1L, entry));

        // Il secondo inserimento fallisce.
        // L'offset ora è 64.
        // Controllo: (offset 64 + size 60) > 100  -->  124 > 100 è VERO. La cache è piena.
        assertFalse(cache.put(1L, 2L, entry));
    }


    /**
     * Simula il caso in cui una entry viene inserita al confine di un segmento di cache,
     * costringendo la logica a trovare una nuova posizione nel segmento successivo.
     */
    @Test
    void testPutEntryCrossingSegmentBoundary() {
        cache = new WriteCache(allocator, 512, 256);
        byte[] largeData = new byte[200];
        ByteBuf largeEntry = Unpooled.wrappedBuffer(largeData);

        // Inserisce una entry che quasi riempie il primo segmento
        assertTrue(cache.put(1L, 1L, largeEntry));

        byte[] smallData = "small-data".getBytes();
        ByteBuf smallEntry = Unpooled.wrappedBuffer(smallData);

        // Questo inserimento dovrebbe "saltare" all'inizio del segmento successivo
        assertTrue(cache.put(2L, 1L, smallEntry));

        // Verifica che entrambe le entry siano recuperabili
        ByteBuf retrievedLarge = cache.get(1L, 1L);
        assertNotNull(retrievedLarge);
        assertEquals(200, retrievedLarge.readableBytes());
        retrievedLarge.release();

        ByteBuf retrievedSmall = cache.get(2L, 1L);
        assertNotNull(retrievedSmall);
        assertEquals(smallData.length, retrievedSmall.readableBytes());
        retrievedSmall.release();
    }

    /**
     * Testa l'aggiornamento dell'ultima entry per un ledger.
     * Se una entry con ID più alto arriva dopo, `lastEntryMap` deve essere aggiornata.
     */
    @Test
    void testLastEntryUpdate() {
        cache = new WriteCache(allocator, 1024, 256);
        ByteBuf entry1 = Unpooled.wrappedBuffer("entry-1".getBytes());
        ByteBuf entry3 = Unpooled.wrappedBuffer("entry-3".getBytes());

        assertTrue(cache.put(1L, 1L, entry1));
        assertTrue(cache.put(1L, 3L, entry3));

        // Ora, inseriamo una entry con ID più basso (out-of-order)
        ByteBuf entry2 = Unpooled.wrappedBuffer("entry-2".getBytes());
        assertTrue(cache.put(1L, 2L, entry2));

        // `getLastEntry` dovrebbe comunque restituire la entry con l'ID più alto (3)
        ByteBuf lastEntry = cache.getLastEntry(1L);
        assertNotNull(lastEntry);

        byte[] lastData = new byte[lastEntry.readableBytes()];
        lastEntry.readBytes(lastData);
        assertArrayEquals("entry-3".getBytes(), lastData);

        lastEntry.release();
    }

    /**
     * Testa il comportamento del metodo `put` in un ambiente multithread
     * per assicurarsi che non ci siano race condition e che i dati siano consistenti.
     */
    @Test
    void testConcurrentPutAndGet() throws InterruptedException {
        cache = new WriteCache(allocator, 10 * 1024 * 1024, 1024 * 1024); // Cache da 10MB, segmenti da 1MB
        int numThreads = 8;
        int entriesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < entriesPerThread; j++) {
                        long ledgerId = threadId;
                        long entryId = j;
                        byte[] data = ("thread-" + threadId + "-entry-" + j).getBytes();
                        ByteBuf entry = Unpooled.wrappedBuffer(data);
                        cache.put(ledgerId, entryId, entry);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Verifica che tutte le entry inserite siano leggibili correttamente
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < entriesPerThread; j++) {
                byte[] expectedData = ("thread-" + i + "-entry-" + j).getBytes();
                ByteBuf retrieved = cache.get(i, j);
                assertNotNull(retrieved, "Entry non trovata per ledger " + i + ", entry " + j);
                byte[] retrievedData = new byte[retrieved.readableBytes()];
                retrieved.readBytes(retrievedData);
                assertArrayEquals(expectedData, retrievedData);
                retrieved.release();
            }
        }

        assertEquals(numThreads * entriesPerThread, cache.count());
    }
}