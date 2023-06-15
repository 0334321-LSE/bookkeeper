package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;

/** Here there are some tests added to increment jacoco coverage and for killing some mutation */
public class WriteCacheAddedTest {
    private WriteCache writeCache;
    private final ByteBufAllocator byteBufAllocator;

    private int entrySize;
    private ByteBuf entry;


    @Before
    public void setUp() throws Exception {
        int entryNumber = 2;
        this.entrySize = 64;
        writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);
    }

    public WriteCacheAddedTest() {
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        this.entry = this.byteBufAllocator.buffer(this.entrySize);
        this.entry.writeBytes("RHEngb7GUMiIrVIRPi44gMjNsSgNO63L5bGy6oFNhBtKk0XDQ1YIIvEROrbUGD20".getBytes());

    }

    @Test @Ignore
    public void PutDifferentOrderTest(){
        boolean putResult;

            this.writeCache.put(0,1,this.entry);
            putResult = this.writeCache.put(0,0,this.entry);

            System.out.println("Expected result: "+ true +"\t|\tResult: "+putResult);
            System.out.println("----------------------------------------");
            Assert.assertTrue(putResult);
    }
    @Test @Ignore
    public void PutIntoFullCacheTest(){
        boolean putResult;
        this.writeCache.put(0,0,this.entry);
        this.writeCache.put(0,1,this.entry);
        putResult = this.writeCache.put(0,2,this.entry);
        System.out.println("Expected result: "+ false +"\t|\tResult: "+putResult);
        System.out.println("----------------------------------------");
        Assert.assertFalse(putResult);
    }

    @After
    public void tearDown() throws Exception {
        writeCache.clear();
        if(entry != null ) entry.release();
        writeCache.close();
    }
}
