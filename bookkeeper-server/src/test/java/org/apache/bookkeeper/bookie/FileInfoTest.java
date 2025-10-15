package org.apache.bookkeeper.bookie;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

public class FileInfoTest {

    // Variabili per il setup
    private FileInfo fileInfo;
    private File currentFile;
    private File newLocationFile;

    private static final String USER_DIR = System.getProperty("user.dir");
    private static final String TEST_RESOURCES_DIR = "src/test/resources/fileinfo/";
    private static final String CURRENT_FILE_PATH = TEST_RESOURCES_DIR + "current_file.txt";
    private static final String NEW_LOCATION_FILE_PATH = TEST_RESOURCES_DIR + "new_location_file.txt";

    private static final long CURRENT_FILE_SIZE = 1024;

    // Possibili esiti attesi
    private enum expectedOutputType {
        EMPTY_FILE_COPIED,
        PARTIAL_FILE_COPIED,
        FULL_FILE_COPIED,
        NO_OPERATION_PERFORMED
    }

    // Tipi di file di destinazione
    private enum newFileType {
        NOT_EXISTING_FILE, // Percorso non valido simulato usando come path una directory
        EXISTING_FILE,     // Percorso valido
        SAME_FILE;         // Stesso percorso del file corrente

        public File getNewFileType() {
            switch (this) {
                case NOT_EXISTING_FILE:
                    return new File(USER_DIR, TEST_RESOURCES_DIR);
                case EXISTING_FILE:
                    return new File(USER_DIR, NEW_LOCATION_FILE_PATH);
                case SAME_FILE:
                    return new File(USER_DIR, CURRENT_FILE_PATH);
                default:
                    return null;
            }
        }
    }

    // Tipi di dimensioni dei file
    private enum sizeType {
        NEGATIVE_SIZE(-1L),
        ZERO_SIZE(0L),
        LONG_MAX_VALUE(Long.MAX_VALUE),
        LOWER_CURRENT_FILE_SIZE(CURRENT_FILE_SIZE - 1),
        GREATER_CURRENT_FILE_SIZE(CURRENT_FILE_SIZE + 1);

        private final long size;

        sizeType(long size) {
            this.size = size;
        }

        public long getSizeType() {
            return size;
        }
    }

    @BeforeEach
    public void setUp() throws IOException {
        File testDir = new File(USER_DIR, TEST_RESOURCES_DIR);
        if (!testDir.exists()) {
            testDir.mkdirs();
        }

        // Prepara il file sorgente con un header valido
        currentFile = new File(USER_DIR, CURRENT_FILE_PATH);
        if(currentFile.exists()) currentFile.delete();
        currentFile.createNewFile();

        byte[] masterKey = new byte[]{};
        int fileInfoVersion = 1;

        try (FileOutputStream fos = new FileOutputStream(currentFile)) {
            ByteBuffer header = ByteBuffer.allocate((int) CURRENT_FILE_SIZE);
            header.putInt(FileInfo.SIGNATURE);
            header.putInt(FileInfo.CURRENT_HEADER_VERSION);
            header.putInt(masterKey.length);
            header.put(masterKey);
            header.putInt(0); // state bits
            header.rewind();
            fos.write(header.array(), 0, (int) CURRENT_FILE_SIZE);
        }

        fileInfo = new FileInfo(currentFile, masterKey, fileInfoVersion);
        newLocationFile = new File(USER_DIR, NEW_LOCATION_FILE_PATH);
        if (newLocationFile.exists()) newLocationFile.delete();
    }

    @AfterEach
    public void tearDown() {
        // Pulisce i file creati durante il test
        if (currentFile != null && currentFile.exists()) {
            currentFile.delete();
        }
        if (newLocationFile != null && newLocationFile.exists()) {
            newLocationFile.delete();
        }

        File rlocFile = new File(newLocationFile.getParentFile(), newLocationFile.getName() + IndexPersistenceMgr.RLOC);
        if (rlocFile.exists()) rlocFile.delete();

        File strayRlocFile = new File(USER_DIR, TEST_RESOURCES_DIR + "fileinfo.rloc");
        if (strayRlocFile.exists()) {
            strayRlocFile.delete();
        }
    }

    private static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(newFileType.EXISTING_FILE.getNewFileType(), sizeType.NEGATIVE_SIZE.getSizeType(), expectedOutputType.EMPTY_FILE_COPIED, null),                  // Caso 1
                Arguments.of(newFileType.EXISTING_FILE.getNewFileType(), sizeType.ZERO_SIZE.getSizeType(), expectedOutputType.EMPTY_FILE_COPIED, null),                  // Caso 2
                Arguments.of(null, sizeType.ZERO_SIZE.getSizeType(), null, NullPointerException.class), // Caso 3
                Arguments.of(newFileType.EXISTING_FILE.getNewFileType(), sizeType.LOWER_CURRENT_FILE_SIZE.getSizeType(), expectedOutputType.PARTIAL_FILE_COPIED, null),                  // Caso 4
                Arguments.of(newFileType.EXISTING_FILE.getNewFileType(), sizeType.GREATER_CURRENT_FILE_SIZE.getSizeType(), expectedOutputType.FULL_FILE_COPIED, null),                  // Caso 5
                Arguments.of(newFileType.NOT_EXISTING_FILE.getNewFileType(), sizeType.LONG_MAX_VALUE.getSizeType(), null, IOException.class),     // Caso 6
                Arguments.of(newFileType.EXISTING_FILE.getNewFileType(), sizeType.LONG_MAX_VALUE.getSizeType(), expectedOutputType.FULL_FILE_COPIED, null),                   // Caso 7
                // Dopo Jacoco
                Arguments.of(newFileType.SAME_FILE.getNewFileType(), sizeType.ZERO_SIZE.getSizeType(), expectedOutputType.NO_OPERATION_PERFORMED, null)    //Caso 8
        );
    }

    @ParameterizedTest
    @MethodSource("data")
    public void moveToNewLocationBoundaryTest(File newFile, long size, expectedOutputType expectedOutput, Class<? extends Throwable> expectedException) throws IOException {
        // Gestione dei casi di fallimento
        if (expectedException != null) {
            assertThrows(expectedException, () -> fileInfo.moveToNewLocation(newFile, size));
        } else {
            // Gestione dei casi di successo
            fileInfo.moveToNewLocation(newFile, size);

            if (expectedOutput == expectedOutputType.NO_OPERATION_PERFORMED) {
                assertTrue(currentFile.exists());
            } else {
                assertFalse(currentFile.exists());
                assertTrue(newFile.exists());

                long newFileSize = newFile.length();
                switch (expectedOutput) {
                    case EMPTY_FILE_COPIED:
                        assertEquals(0, newFileSize);
                        break;
                    case PARTIAL_FILE_COPIED:
                        assertEquals(size, newFileSize);
                        break;
                    case FULL_FILE_COPIED:
                        assertEquals(CURRENT_FILE_SIZE, newFileSize);
                        break;
                }
            }
        }
    }

    // Test aggiuntivi dopo Jacoco
    @Test
    public void testNullChannelOrSameFile() throws Exception{

        // Usa la Reflection di Java per accedere a un campo privato ('fc') e impostarlo a null
        Field fcField = FileInfo.class.getDeclaredField("fc");
        fcField.setAccessible(true);
        fcField.set(fileInfo, null);

        fileInfo.moveToNewLocation(currentFile, 0);
    }

    @Test
    public void testTransferToFailure() throws Exception{
        // Crea un mock di FileChannel e simula un errore di copia dei dati
        FileChannel mockFc = Mockito.mock(FileChannel.class);
        when(mockFc.size()).thenReturn(1024L);
        when(mockFc.transferTo(anyLong(), anyLong(), any(FileChannel.class))).thenReturn(0L);

        // Usa la reflection per usare il mock all'interno dell'oggetto FileInfo
        Field fcField = FileInfo.class.getDeclaredField("fc");
        fcField.setAccessible(true);
        fcField.set(fileInfo, mockFc);

        assertThrows(IOException.class, () -> fileInfo.moveToNewLocation(newLocationFile, 1024L));
    }

    @Test
    public void testDeleteFailure(){
        // Crea un oggetto spy
        FileInfo spyFileInfo = Mockito.spy(fileInfo);
        // Non si esegue il codice della funzione delete(), ma si restituisce valore false simulando un fallimento nella cancellazione
        Mockito.doReturn(false).when(spyFileInfo).delete();

        assertThrows(IOException.class, () -> spyFileInfo.moveToNewLocation(newLocationFile, 0));
    }

    // Test aggiuntivi dopo PIT
    @Test
    public void testRlocFileAlreadyExists() throws IOException {
        // Uccide la mutazione che nega la condizione `if (!rlocFile.exists())`
        // Crea manualmente il file .rloc per simulare uno stato pre-esistente
        File rlocFile = new File(newLocationFile.getParentFile(), newLocationFile.getName() + IndexPersistenceMgr.RLOC);
        assertTrue(rlocFile.createNewFile());
        assertDoesNotThrow(() -> fileInfo.moveToNewLocation(newLocationFile, 10));
        assertTrue(newLocationFile.exists());
    }

    @Test
    public void testMoveToNonExistentDirectory() throws IOException {
        // Uccide la mutazione che rimuove la chiamata a `checkParents`
        // Tenta di spostare il file in una sottodirectory che non esiste
        File nonExistentDir = new File(USER_DIR, TEST_RESOURCES_DIR + "subdir/");
        File targetFile = new File(nonExistentDir, "target.txt");
        if (nonExistentDir.exists()) {
            for (File f : nonExistentDir.listFiles()) f.delete();
            nonExistentDir.delete();
        }
        assertDoesNotThrow(() -> fileInfo.moveToNewLocation(targetFile, 10));
        assertTrue(targetFile.exists());
        targetFile.delete();
        nonExistentDir.delete();
    }

    @Test
    public void testForceAndCloseAreCalled() throws Exception {
        // Uccide le mutazioni che rimuovono le chiamate a `.force()` e `.close()`
        // Prepara un mock per il canale originale per simulare una copia riuscita
        FileChannel mockOriginalFc = Mockito.mock(FileChannel.class);
        when(mockOriginalFc.size()).thenReturn(1024L);
        when(mockOriginalFc.transferTo(anyLong(), anyLong(), any(FileChannel.class))).thenReturn(1024L);
        Field fcField = FileInfo.class.getDeclaredField("fc");
        fcField.setAccessible(true);
        fcField.set(fileInfo, mockOriginalFc);

        // Intercetta la creazione del nuovo file channel ed esegue il metodo
        try (MockedConstruction<RandomAccessFile> mockedRaf = Mockito.mockConstruction(RandomAccessFile.class,
                (mock, context) -> {
                    // Crea un secondo mock per il canale del file di destinazione
                    FileChannel mockNewFc = Mockito.mock(FileChannel.class);
                    when(mock.getChannel()).thenReturn(mockNewFc);
                })) {
            fileInfo.moveToNewLocation(newLocationFile, 1024L);
            // Verifica la chiamata dei metodi sul nuovo canale
            RandomAccessFile mockedFile = mockedRaf.constructed().get(0);
            FileChannel mockedChannel = mockedFile.getChannel();
            Mockito.verify(mockedChannel).force(true);
            Mockito.verify(mockedChannel).close();
        }
        Mockito.verify(mockOriginalFc).close();
    }

}

