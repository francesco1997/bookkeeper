package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadTest {

    private static String dir = "tmp/readChannelTest";
    private static String fileName = "readFile.log";

    private boolean isWriteInFile;
    private Integer fileSize;
    private Integer dataToRead;
    private Integer startIndex;
    private Integer buffChanCapacity;

    private FileChannel fc;
    private BufferedChannel bufferedChannel;
    private byte[] bytes;


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public BufferedChannelReadTest(TestInput testInput) {
        this.isWriteInFile = testInput.isWriteInFile();
        this.fileSize = testInput.getFileSize();
        this.dataToRead = testInput.getDataToRead();
        this.startIndex = testInput.getStartIndex();
        this.buffChanCapacity = testInput.getBuffChanCapacity();
        
        if (testInput.getExpectedException() != null) {
            this.expectedException.expect(testInput.getExpectedException());
        }
    }



    @Parameterized.Parameters
    public static Collection<TestInput> getTestParameters() {
        List<TestInput> inputs = new ArrayList<>();

        /*
            fileSize: {>0}, {=0}
            buffChanCapacity: {<0}, {=0}, {>0}
            startIndex: {<fileSize}, {=fileSize}, {>fileSize}
            dataToRead: {<fileSize - startIndex}, {=fileSize - startIndex}, {>fileSize - startIndex}
         */
        inputs.add(new TestInput(true,1,0,2,-1, IllegalArgumentException.class));
        inputs.add(new TestInput(true,0,0,0,0, null));
        inputs.add(new TestInput(true,2,1,0,1, null));
        inputs.add(new TestInput(false,5,1,0,1, null));
        inputs.add(new TestInput(false,5,6,0,1, IOException.class));
        inputs.add(new TestInput(false,5,1,3,6, null));
        inputs.add(new TestInput(false,5,6,0,10, IOException.class));

        return inputs;
    }

    private static class TestInput {
        private boolean writeInFile;
        private Integer fileSize;
        private Integer dataToRead;
        private Integer startIndex;
        private Integer buffChanCapacity;
        private Class<? extends Exception> expectedException;


        protected TestInput(boolean writeInFile, Integer fileSize, Integer dataToRead, Integer startIndex, Integer buffChanCapacity, Class<? extends Exception> expectedException) {
            this.writeInFile = writeInFile;
            this.fileSize = fileSize;
            this.dataToRead = dataToRead;
            this.startIndex = startIndex;
            this.buffChanCapacity = buffChanCapacity;
            this.expectedException = expectedException;

        }

        public boolean isWriteInFile() {
            return writeInFile;
        }

        public Integer getFileSize() {
            return fileSize;
        }

        public Integer getDataToRead() {
            return dataToRead;
        }

        public Integer getStartIndex() {
            return startIndex;
        }

        public Integer getBuffChanCapacity() {
            return buffChanCapacity;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }

    @BeforeClass
    static public void setupEnvironment() {
        // Create the directories if do not exist
        if (!Files.exists(Paths.get("tmp"))) {
            File tmpDir = new File("tmp");
            tmpDir.mkdir();
        }

        if (!Files.exists(Paths.get("tmp","readChannelTest"))) {
            File tmpDir = new File("tmp", "readChannelTest");
            tmpDir.mkdir();
        }

        // Delete test file if exists
        if (Files.exists(Paths.get(dir, fileName))) {
            File testFile = new File(dir,fileName);
            testFile.delete();
        }
    }

    @Before
    public void setup() throws IOException {
        Random rd = new Random();
        this.bytes = new byte[this.fileSize];
        rd.nextBytes(this.bytes);
        if (this.isWriteInFile) {
            try (FileOutputStream fos = new FileOutputStream(dir + "/" + fileName)) {
                fos.write(this.bytes);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        this.fc = FileChannel.open(Paths.get(dir, fileName), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.fc.position(this.fc.size());

    }

    @After
    public void tearDown() {
        // Delete the file
        try {
            this.fc.close();
            // Delete test file if exists
            if (Files.exists(Paths.get(dir, fileName))) {
                File testFile = new File(dir,fileName);
                testFile.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test//(timeout=1000)
    public void bufChWrTest() throws Exception {
        ByteBuf readBuf = Unpooled.buffer();
        this.bufferedChannel = new BufferedChannel(new UnpooledByteBufAllocator(true), this.fc, this.buffChanCapacity);

        if (!this.isWriteInFile) {
            ByteBuf writeBuf = Unpooled.buffer();
            writeBuf.writeBytes(this.bytes);
            this.bufferedChannel.write(writeBuf);
        }

        Integer numReadBytes = this.bufferedChannel.read(readBuf, this.startIndex, this.dataToRead); // Errore: non tiene conto della length messa in input

        Integer numReadBytesExpected = 0;
        byte[] expectedBytes = new byte[0];
        if (this.startIndex <= this.bytes.length) {
            numReadBytesExpected =  ( this.bytes.length - this.startIndex >= this.dataToRead) ?  this.dataToRead : this.bytes.length - this.startIndex - this.dataToRead;
            if (this.dataToRead > 0) {
                expectedBytes = Arrays.copyOfRange(this.bytes, this.startIndex, this.startIndex + numReadBytesExpected);
            }
        } else {
            numReadBytesExpected = -1;
        }

        byte[] readBytes = Arrays.copyOfRange(readBuf.array(), 0, numReadBytes);

        Assert.assertEquals("Read error", numReadBytesExpected, numReadBytes);

        Assert.assertEquals("Error", Arrays.toString(expectedBytes), Arrays.toString(readBytes));
    }


}
