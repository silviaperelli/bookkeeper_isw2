package org.apache.bookkeeper.bookie;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(value = Parameterized.class)
public class FileInfoTest {

    // Parametri del test
    private final File newFile;
    private final long size;

    private final expectedOutputType expectedOutput;
    private final Class<? extends Throwable> expectedException;

    // Variabili per il setup
    private FileInfo fileInfo;
    private File currentFile;
    private File newLocationFile;

    private static final String USER_DIR = System.getProperty("user.dir");
    private static final String CURRENT_FILE_PATH = "src/test/resources/fileinfo/current_file.txt";
    private static final String NEW_LOCATION_FILE_PATH = "src/test/resources/fileinfo/new_location_file.txt";

    private static final long CURRENT_FILE_SIZE = 1024;

    // Costruttore per i parametri
    public FileInfoTest(File newFile, long size, expectedOutputType expectedOutput, Class<? extends Throwable> expectedException) {
        this.newFile = newFile;
        this.size = size;
        this.expectedOutput = expectedOutput;
        this.expectedException = expectedException;
    }

    private enum expectedOutputType {
        EMPTY_FILE_COPIED,
        PARTIAL_FILE_COPIED,
        FULL_FILE_COPIED
    }

    private enum newFileType {
        NOT_EXISTING_FILE,
        EXISTING_FILE;

        public File getNewFileType() {
            switch (this) {
                case NOT_EXISTING_FILE:
                    return new File(USER_DIR, "src/test/resources/fileinfo");
                case EXISTING_FILE:
                    return new File(USER_DIR, "src/test/resources/fileinfo/new_location_file.txt");
                default:
                    return null;
            }
        }
    }

    private enum sizeType {
        NEGATIVE_SIZE(-1L),
        ZERO_SIZE(0L),
        LONG_MAX_VALUE(Long.MAX_VALUE),
        LOWER_CURRENT_FILE_SIZE(CURRENT_FILE_SIZE - 1),
        GREATER_CURRENT_FILE_SIZE(CURRENT_FILE_SIZE + 1);

        private final long size;
        sizeType(long size) { this.size = size; }
        public long getSizeType() { return size; }
    }

    @Before
    public void setUp() throws IOException {
        // Prepara il file sorgente con un header valido
        currentFile = new File(USER_DIR, CURRENT_FILE_PATH);
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
    }

    @After
    public void tearDown() {
        // Pulisce i file dopo ogni test
        if (currentFile != null && currentFile.exists()) {
            currentFile.delete();
        }
        if (newLocationFile != null && newLocationFile.exists()) {
            newLocationFile.delete();
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {newFileType.EXISTING_FILE.getNewFileType(), sizeType.NEGATIVE_SIZE.getSizeType(), expectedOutputType.EMPTY_FILE_COPIED,   null},                  // Caso 1
                {newFileType.EXISTING_FILE.getNewFileType(), sizeType.ZERO_SIZE.getSizeType(), expectedOutputType.EMPTY_FILE_COPIED,   null},                  // Caso 2
                {null, sizeType.ZERO_SIZE.getSizeType(), null, NullPointerException.class}, // Caso 3
                {newFileType.EXISTING_FILE.getNewFileType(), sizeType.LOWER_CURRENT_FILE_SIZE.getSizeType(), expectedOutputType.PARTIAL_FILE_COPIED, null},                  // Caso 4
                {newFileType.EXISTING_FILE.getNewFileType(), sizeType.GREATER_CURRENT_FILE_SIZE.getSizeType(), expectedOutputType.FULL_FILE_COPIED,    null},                  // Caso 5
                {newFileType.NOT_EXISTING_FILE.getNewFileType(), sizeType.LONG_MAX_VALUE.getSizeType(), null, IOException.class},     // Caso 6
                {newFileType.EXISTING_FILE.getNewFileType(), sizeType.LONG_MAX_VALUE.getSizeType(), expectedOutputType.FULL_FILE_COPIED, null}                   // Caso 7
        });
    }

    @Test
    public void moveToNewLocationBoundaryTest() throws IOException {
        if (expectedException != null) {
            assertThrows(expectedException, () -> fileInfo.moveToNewLocation(newFile, size));
        } else {
            fileInfo.moveToNewLocation(newFile, size);

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