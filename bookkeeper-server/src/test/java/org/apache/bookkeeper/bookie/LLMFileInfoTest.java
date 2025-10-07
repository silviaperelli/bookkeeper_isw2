package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test suite per il metodo moveToNewLocation della classe FileInfo.
 * Utilizza JUnit e una directory temporanea per i test basati su file.
 */
class LLMFileInfoTest {

    // Directory temporanea gestita da JUnit, si pulisce automaticamente dopo i test.
    @TempDir
    Path tempDir;

    // Chiave fittizia per i test.
    private static final byte[] MASTER_KEY = "test-master-key".getBytes(StandardCharsets.UTF_8);

    // Il suffisso .rloc è usato internamente dal metodo moveToNewLocation.
    private static final String RLOC_SUFFIX = ".rloc";

    private File sourceFile;
    private FileInfo fileInfo;

    @BeforeEach
    void setUp() throws IOException {
        // Inizializza un file di origine prima di ogni test.
        sourceFile = tempDir.resolve("testLedger.idx").toFile();
    }

    /**
     * Test del caso principale: il file viene spostato correttamente in una nuova posizione.
     * Si verifica che il file originale venga eliminato, il nuovo file esista,
     * il contenuto sia corretto e lo stato interno dell'oggetto FileInfo sia aggiornato.
     */
    @Test
    void testMoveToNewLocation_HappyPath_FullCopy() throws IOException {
        // Prepara il file di origine con un header valido e 100 byte di dati.
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);
        long originalFileSize = sourceFile.length();

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader(); // Legge l'header per inizializzare lo stato interno.

        File destFile = tempDir.resolve("movedLedger.idx").toFile();

        // Sposta il file nella nuova posizione, richiedendo la copia dell'intero contenuto.
        fileInfo.moveToNewLocation(destFile, Long.MAX_VALUE);

        assertFalse(sourceFile.exists(), "Il file di origine non dovrebbe più esistere.");
        assertTrue(destFile.exists(), "Il file di destinazione dovrebbe esistere.");
        assertEquals(originalFileSize, destFile.length(), "La dimensione del file di destinazione deve corrispondere all'originale.");

        // Verifica che il contenuto copiato sia corretto.
        byte[] movedContent = Files.readAllBytes(destFile.toPath());
        assertArrayEquals(content, readContentFromMovedFile(movedContent), "Il contenuto del file spostato non è corretto.");

        // Verifica che lo stato interno dell'oggetto FileInfo sia stato aggiornato.
        assertTrue(fileInfo.isSameFile(destFile), "FileInfo dovrebbe puntare al nuovo file.");
    }

    /**
     * Test dello spostamento di una porzione del file.
     * Verifica che solo il numero specificato di byte di dati (più l'header) venga copiato.
     */
    @Test
    void testMoveToNewLocation_PartialCopy() throws IOException {
        byte[] content = generateContent(200);
        createAndSetupFile(sourceFile, content);

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        File destFile = tempDir.resolve("partialMovedLedger.idx").toFile();
        long dataSizeToCopy = 50;
        // La dimensione totale da copiare include l'header.
        long totalSizeToCopy = FileInfo.START_OF_DATA + dataSizeToCopy;

        fileInfo.moveToNewLocation(destFile, totalSizeToCopy);

        assertFalse(sourceFile.exists(), "Il file di origine non dovrebbe più esistere.");
        assertTrue(destFile.exists(), "Il file di destinazione dovrebbe esistere.");
        assertEquals(totalSizeToCopy, destFile.length(), "La dimensione del file di destinazione non è corretta.");

        byte[] expectedContent = new byte[(int) dataSizeToCopy];
        System.arraycopy(content, 0, expectedContent, 0, (int) dataSizeToCopy);

        byte[] movedContent = Files.readAllBytes(destFile.toPath());
        assertArrayEquals(expectedContent, readContentFromMovedFile(movedContent), "Il contenuto parziale non è stato copiato correttamente.");
    }

    /**
     * Testa il caso in cui la dimensione richiesta per lo spostamento sia maggiore
     * della dimensione effettiva del file. Il metodo dovrebbe copiare l'intero file.
     */
    @Test
    void testMoveToNewLocation_SizeGreaterThanFile() throws IOException {
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);
        long originalFileSize = sourceFile.length();

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        File destFile = tempDir.resolve("movedLedger.idx").toFile();

        // Richiedi di copiare più byte di quanti ce ne siano nel file.
        fileInfo.moveToNewLocation(destFile, originalFileSize + 500);

        assertFalse(sourceFile.exists());
        assertTrue(destFile.exists());
        assertEquals(originalFileSize, destFile.length());
    }

    /**
     * Test dello spostamento con una dimensione pari a quella del solo header (0 byte di dati).
     * Dovrebbe essere creato un nuovo file contenente solo l'header.
     */
    @Test
    void testMoveToNewLocation_HeaderOnlyCopy() throws IOException {
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        File destFile = tempDir.resolve("zeroMovedLedger.idx").toFile();

        // Sposta solo la parte di file che corrisponde all'header.
        fileInfo.moveToNewLocation(destFile, FileInfo.START_OF_DATA);

        assertFalse(sourceFile.exists(), "Il file di origine non dovrebbe più esistere.");
        assertTrue(destFile.exists(), "Il file di destinazione dovrebbe esistere.");
        assertEquals(FileInfo.START_OF_DATA, destFile.length(), "Il file di destinazione dovrebbe contenere solo l'header.");

        // Verifica che la parte dati sia vuota.
        byte[] movedContent = Files.readAllBytes(destFile.toPath());
        assertEquals(0, readContentFromMovedFile(movedContent).length, "Non dovrebbe esserci contenuto dati nel nuovo file.");
    }

    /**
     * Testa il caso in cui il file di destinazione è lo stesso di quello di origine.
     * Il metodo dovrebbe uscire immediatamente senza fare nulla.
     */
    @Test
    void testMoveToNewLocation_SameFile() throws IOException {
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);
        long originalFileSize = sourceFile.length();

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        long lastModifiedBefore = sourceFile.lastModified();

        fileInfo.moveToNewLocation(sourceFile, 100);

        assertTrue(sourceFile.exists(), "Il file sorgente dovrebbe ancora esistere.");
        assertEquals(originalFileSize, sourceFile.length(), "La dimensione del file non deve cambiare.");
        assertEquals(lastModifiedBefore, sourceFile.lastModified(), "Il file non avrebbe dovuto essere modificato.");
    }

    /**
     * Testa lo scenario in cui la rinomina del file temporaneo (.rloc) fallisce.
     * Il metodo dovrebbe lanciare una IOException.
     */
    @Test
    void testMoveToNewLocation_FailsToRename() throws IOException {
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        // Causa il fallimento della rinomina creando una directory con lo stesso nome del file di destinazione.
        File destFileAsDir = tempDir.resolve("destIsDir").toFile();
        assertTrue(destFileAsDir.mkdir(), "La creazione della directory per il test è fallita.");

        IOException e = assertThrows(IOException.class, () -> {
            fileInfo.moveToNewLocation(destFileAsDir, 100);
        });

        assertTrue(e.getMessage().contains("Failed to rename"), "Il messaggio di errore non è quello previsto.");

        File rlocFile = new File(destFileAsDir.getParentFile(), destFileAsDir.getName() + RLOC_SUFFIX);
        assertTrue(rlocFile.exists(), "Il file .rloc temporaneo dovrebbe esistere ancora dopo il fallimento.");
        assertFalse(sourceFile.exists(), "Il file di origine dovrebbe essere stato cancellato anche in caso di fallimento della rinomina.");
    }

    // --- Metodi Helper ---

    /**
     * Crea un file, scrive un header valido e completo (1024 byte) e il contenuto specificato.
     */
    private void createAndSetupFile(File file, byte[] content) throws IOException {
        try (FileChannel fc = new RandomAccessFile(file, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate((int) FileInfo.START_OF_DATA);
            header.putInt(FileInfo.SIGNATURE);
            header.putInt(FileInfo.CURRENT_HEADER_VERSION);
            header.putInt(MASTER_KEY.length);
            header.put(MASTER_KEY);
            header.putInt(0); // stateBits
            header.putInt(0); // explicitLac length for V1

            // Assicura che l'intero buffer da 1024 byte sia scritto, non solo i byte inseriti.
            header.rewind();
            fc.write(header);

            if (content.length > 0) {
                fc.write(ByteBuffer.wrap(content));
            }
            fc.force(true); // Forza la scrittura su disco per evitare race condition nei test.
        }
    }

    /**
     * Genera un array di byte con un contenuto di test prevedibile.
     */
    private byte[] generateContent(int size) {
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) (i % 256);
        }
        return content;
    }

    /**
     * Estrae il contenuto "utile" da un file spostato, saltando l'header.
     */
    private byte[] readContentFromMovedFile(byte[] movedFileBytes) {
        int contentSize = movedFileBytes.length - (int) FileInfo.START_OF_DATA;
        if (contentSize <= 0) {
            return new byte[0];
        }
        byte[] content = new byte[contentSize];
        System.arraycopy(movedFileBytes, (int) FileInfo.START_OF_DATA, content, 0, contentSize);
        return content;
    }

    /**
     * Uccide il mutante che rimuove la chiamata a checkParents().
     */
    @Test
    void testMoveToNewLocation_WithNonExistentParentDirectory() throws IOException {
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);
        long originalFileSize = sourceFile.length();

        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        // La destinazione è in una sotto-directory che non esiste.
        File destFile = tempDir.resolve("new/subdir/moved.idx").toFile();
        assertFalse(destFile.getParentFile().exists(), "La directory genitore non dovrebbe esistere prima del test.");

        fileInfo.moveToNewLocation(destFile, originalFileSize);

        assertTrue(destFile.exists(), "Il file di destinazione dovrebbe essere stato creato.");
        assertEquals(originalFileSize, destFile.length(), "La dimensione del file di destinazione deve essere corretta.");
    }

    /**
     * Uccide il mutante che nega la condizione !delete().
     */
    @Test
    void testMoveToNewLocation_FailsToDeleteSourceFile() throws IOException {
        // Creiamo directory separate per sorgente e destinazione.
        File sourceDir = tempDir.resolve("source").toFile();
        assertTrue(sourceDir.mkdir(), "Creazione della directory sorgente fallita.");
        File destDir = tempDir.resolve("dest").toFile();
        assertTrue(destDir.mkdir(), "Creazione della directory di destinazione fallita.");

        // Il file di origine è nella sua directory.
        File sourceFileInSubdir = new File(sourceDir, "sourceLedger.idx");

        byte[] content = generateContent(100);
        createAndSetupFile(sourceFileInSubdir, content);

        fileInfo = new FileInfo(sourceFileInSubdir, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        // Il file di destinazione è nell'altra directory.
        File destFileInSubdir = new File(destDir, "destLedger.idx");

        // Rendiamo solo la directory di origine di sola lettura
        assertTrue(sourceDir.setReadOnly(), "Impossibile impostare la directory sorgente come read-only.");

        try {
            IOException e = assertThrows(IOException.class, () -> {
                fileInfo.moveToNewLocation(destFileInSubdir, sourceFileInSubdir.length());
            });

            assertTrue(e.getMessage().contains("Failed to delete the previous index file"),
                    "Messaggio di errore non corretto: " + e.getMessage());

        } finally {
            // Pulizia
            assertTrue(sourceDir.setWritable(true), "Impossibile ripristinare i permessi di scrittura.");
        }
    }

    /**
     * Uccide il mutante che rimuove la chiamata a checkOpen() all'inizio del metodo.
     */
    @Test
    void testMoveToNewLocation_WhenFileChannelIsNotInitiallyOpen() throws IOException {
        byte[] content = generateContent(100);
        createAndSetupFile(sourceFile, content);
        long originalFileSize = sourceFile.length();

        // Creiamo l'oggetto, ma non chiamiamo readHeader(). 'fc' è null.
        fileInfo = new FileInfo(sourceFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);

        File destFile = tempDir.resolve("lazy-open-move.idx").toFile();

        assertDoesNotThrow(() -> {
            fileInfo.moveToNewLocation(destFile, originalFileSize);
        }, "Il metodo non dovrebbe lanciare una NullPointerException anche se il canale non è aperto.");

        assertTrue(destFile.exists(), "Il file di destinazione dovrebbe essere stato creato.");
        assertEquals(originalFileSize, destFile.length(), "La dimensione del file di destinazione deve essere corretta.");
    }

}