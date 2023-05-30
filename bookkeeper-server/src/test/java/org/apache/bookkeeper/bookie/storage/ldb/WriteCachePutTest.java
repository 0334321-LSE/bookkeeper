package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WriteCachePutTest {
    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;

    private int entrySize;
    private ByteBuf entry;
    private final long ledgerId;
    private final long entryId;
    private final boolean isExpectedException;

    private enum entryType {
        NULL,
        VALID,
        NOT_VALID,
        EMPTY
    }

    @Before
    public void setUp() throws Exception {
        int entryNumber = 10;
        this.entrySize = 1024;
        writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);
    }

    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {
                // ledgerID    entryID   entryType         exception
                {  -1,          -1,     entryType.VALID,   true},
                {  -1,           0,     entryType.VALID,   true},
                {   0,           0,     entryType.VALID,   false},
                {   0,          -1,     entryType.VALID,   true},

                {  -1,          -1,     entryType.NULL,    true},
                {  -1,           0,     entryType.NULL,    true},
                {   0,           0,     entryType.NULL,    true},
                {   0,          -1,     entryType.NULL,    true},

                {  -1,          -1,     entryType.EMPTY,   true},
                {  -1,           0,     entryType.EMPTY,   true},
                {   0,           0,     entryType.EMPTY,   false},
                {   0,          -1,     entryType.EMPTY,   true}
        });
    }

    public WriteCachePutTest(long ledgerId, long entryId, entryType entry, boolean isExpectedException){
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.isExpectedException= isExpectedException;
        byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        switch (entry){
            case NULL:
                this.entry = null;
                break;
            case VALID:
                this.entry = this.byteBufAllocator.buffer(this.entrySize);
                this.entry.writeBytes("bytes into the entry".getBytes());
                break;
            case EMPTY:
                this.entry = this.byteBufAllocator.buffer(this.entrySize);
                break;
            case NOT_VALID:
                //TODO: think how to create an invalid instance
                break;
        }
    }

    @Test
    public void PutTest(){
        boolean putResult;
        try{
            putResult = writeCache.put(this.ledgerId,this.entryId,this.entry);
            //If is expected exception, put must return false
            // otherwise if it isn't expected, put must return true
            System.out.println("Expected result: "+!this.isExpectedException+"\t|\tResult: "+putResult);
            System.out.println("----------------------------------------");
            Assert.assertNotEquals(putResult,this.isExpectedException);
        }catch (Exception e){
            System.out.println("Expected exception: "+this.isExpectedException+"\t|\tResult: "+e.getClass().getName());
            System.out.println("----------------------------------------");
            Assert.assertTrue("Caught exception",this.isExpectedException);
        }
    }

    @After
    public void tearDown() throws Exception {
        writeCache.clear();
        if(entry != null ) entry.release();
        writeCache.close();
    }

}
