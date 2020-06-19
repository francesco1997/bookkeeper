package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

;import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;


@RunWith(value = Parameterized.class)
public class BufferedChannelTest {

    private static String dir = "tmp/writeChannelTest";
    private static String fileName = "writeFile.log";

    private boolean isEmptyFile;
    private Integer writeBufSize;
    private Integer buffChanCapacity;
    private Integer initialPos;

    private FileChannel fc;
    private BufferedChannel bufferedChannel;
    private ByteBuf inputBuf;
    private byte[] bytes;


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public BufferedChannelTest(TestInput testInput) {
        this.isEmptyFile = testInput.isEmptyFile();
        this.writeBufSize = testInput.getWriteBufSize();
        this.buffChanCapacity = testInput.getBuffChanCapacity();
        if (testInput.getExpectedException() != null) {
            this.expectedException.expect(testInput.getExpectedException());
        }
        this.initialPos = 0;

    }



    @Parameterized.Parameters
    public static Collection<TestInput> getTestParameters() {
        List<TestInput> inputs = new ArrayList<>();

        // Empty file, writeDim > 0, srcSize = writeDim
        inputs.add(new TestInput(true, 1, 1, null));
        inputs.add(new TestInput(true, 1, 2, null));
        //inputs.add(new TestInput(true, 1,0, IllegalArgumentException.class));
        //inputs.add(new TestInput(true, 0, 0, IllegalArgumentException.class));
        inputs.add(new TestInput(true, 0, 1, null));
        inputs.add(new TestInput(true, 3, -1, IllegalArgumentException.class));
        inputs.add(new TestInput(false, 1, 1, null));
        inputs.add(new TestInput(false, 1, 2, null));
        //inputs.add(new TestInput(false, 1,0, IllegalArgumentException.class));
        //inputs.add(new TestInput(false, 0, 0, IllegalArgumentException.class));
        inputs.add(new TestInput(false, 0, 1, null));
        inputs.add(new TestInput(false, 0, -1, IllegalArgumentException.class));


        return inputs;
    }

    private static class TestInput {
        private boolean isEmptyFile;
        private Integer writeBufSize;
        private Integer buffChanCapacity;
        private Class<? extends Exception> expectedException;


        protected TestInput(boolean isEmptyFile, Integer writeBufSize, Integer buffChanCapacity, Class<? extends Exception> expectedException) {
            this.isEmptyFile = isEmptyFile;
            this.writeBufSize = writeBufSize;
            this.buffChanCapacity = buffChanCapacity;
            this.expectedException = expectedException;

        }

        public boolean isEmptyFile() {
            return isEmptyFile;
        }

        public Integer getWriteBufSize() {
            return writeBufSize;
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

        if (!Files.exists(Paths.get("tmp","writeChannelTest"))) {
            File tmpDir = new File("tmp", "writeChannelTest");
            tmpDir.mkdir();
        }

        // Delete test file if exists
        if (Files.exists(Paths.get(dir, fileName))) {
            File testFile = new File(dir,fileName);
            testFile.delete();
        }
    }

    @Before
    public void setup() {
        try {
            Random rd = new Random();
            this.bytes = new byte[this.writeBufSize];
            rd.nextBytes(this.bytes);

            if (!this.isEmptyFile) {
                try (FileOutputStream fos = new FileOutputStream(dir + "/" +fileName)) {
                    this.initialPos = rd.nextInt(30);
                    byte[] initialBytes = new byte[this.initialPos];
                    rd.nextBytes(initialBytes);

                    fos.write(initialBytes);
                }
            }

            this.fc = FileChannel.open(Paths.get(dir, fileName), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            this.inputBuf = Unpooled.directBuffer();
            this.inputBuf.writeBytes(this.bytes);

        } catch (IOException e) {
            e.printStackTrace();
        }
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

    @Test(timeout=1000)
    public void bufChWrTest() throws Exception {
        this.bufferedChannel = new BufferedChannel(new UnpooledByteBufAllocator(true), this.fc, this.buffChanCapacity);
        this.bufferedChannel.write(this.inputBuf);

        // Check if the write was made correctly
        Integer numBytesInBuff = 0;
        Integer numBytesInFileCh = 0;
        if (this.writeBufSize > this.buffChanCapacity) {
            numBytesInBuff = this.writeBufSize % this.buffChanCapacity;
            numBytesInFileCh = this.writeBufSize - numBytesInBuff;
        } else {
            numBytesInBuff = this.writeBufSize;
        }

        byte[] bytesInBuf = new byte[numBytesInBuff];
        this.bufferedChannel.writeBuffer.getBytes(0, bytesInBuf);

        byte[] expectedBytes = Arrays.copyOfRange(this.bytes, this.bytes.length - numBytesInBuff, this.bytes.length);

        Assert.assertEquals("Error", Arrays.toString(expectedBytes), Arrays.toString(bytesInBuf));

        ByteBuffer buff = ByteBuffer.allocate(numBytesInFileCh);

        this.fc.position(this.initialPos);
        this.fc.read(buff);
        byte[] bytesInFileCh = buff.array();

        expectedBytes = Arrays.copyOfRange(this.bytes, 0, numBytesInFileCh);
        Assert.assertEquals("Error", Arrays.toString(expectedBytes), Arrays.toString(bytesInFileCh));

    }


}
