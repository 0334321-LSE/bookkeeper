package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WriteCacheJacocoTest {
    private WriteCache writeCache;
    private final ByteBufAllocator byteBufAllocator;

    private int entrySize;
    private ByteBuf entry;
    private final boolean expectedResult;

    @Before
    public void setUp() throws Exception {
        int entryNumber = 10;
        this.entrySize = 64;
        writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);
    }

    public WriteCacheJacocoTest() {
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        this.entry = this.byteBufAllocator.buffer(this.entrySize);
        this.entry.writeBytes("Entry for better jacoco results".getBytes());
        this.expectedResult = true;
    }

    @Test
    public void PutTest(){
        boolean putResult;

            this.writeCache.put(0,1,this.entry);
            putResult = this.writeCache.put(0,0,this.entry);

            //If is expected exception, put must return false
            // otherwise if it isn't expected, put must return true
            System.out.println("Expected result: "+this.expectedResult +"\t|\tResult: "+putResult);
            System.out.println("----------------------------------------");
            Assert.assertEquals(this.expectedResult,putResult);
    }

    @After
    public void tearDown() throws Exception {
        writeCache.clear();
        if(entry != null ) entry.release();
        writeCache.close();
    }
}
