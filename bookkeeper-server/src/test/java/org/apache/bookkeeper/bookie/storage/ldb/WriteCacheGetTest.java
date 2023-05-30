package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
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
public class WriteCacheGetTest {

    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;
    private ByteBuf entry;
    private final long ledgerId = 2;
    private final long entryId = 1;
    private final boolean isValidLedgerId;
    private final boolean isValidEntryId;
    private final boolean isExceptionExpected;


    @Before
    public void setUp() throws Exception {
        byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        int entryNumber = 10;
        int entrySize = 1024;
        writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);

        entry = byteBufAllocator.buffer(entrySize);

        entry.writeBytes("bytes into the entry".getBytes());
       /* String input = new String(ByteBufUtil.getBytes(entry));
        System.out.println(input);*/

/*
        ByteBufUtil.writeAscii(entry, "test");
*/

        writeCache.put(ledgerId, entryId, entry);
    }

    @After
    public void tearDown() throws Exception {
        writeCache.clear();
        entry.release();
        writeCache.close();
    }

    //Input parameters
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {
                // ledgerID     entryID     exception
                {  true,        true,       false},
                {  true,        false,      true},
                {  false,       true,       true},
                {  false,       false,      true}
        });
    }

    public WriteCacheGetTest(boolean isLedgerValid, boolean isEntryValid , boolean isExceptionExpected){
        this.isValidLedgerId = isLedgerValid;
        this.isValidEntryId = isEntryValid;
        this.isExceptionExpected = isExceptionExpected;
    }

    @Test
    public void getFromCacheTest(){

        ByteBuf result = null;

        long  actualLedgerId = ledgerId, actualEntryId = entryId;

        if(!this.isValidLedgerId) actualLedgerId++;
        if(!this.isValidEntryId) actualEntryId++;

        try{
            System.out.println("validLedgerID: "+this.ledgerId  + "\t|\t actualLedgerID: "+actualLedgerId);
            System.out.println("validEntryID:  "+ this.entryId  + "\t|\t actualEntryID:  "+actualEntryId);
            System.out.println("----------------------------------------");
            result = writeCache.get(actualLedgerId, actualEntryId);
        }
        catch(Exception e){
            e.printStackTrace();
            Assert.assertTrue("Caught exception",this.isExceptionExpected);
        }

        if(!this.isExceptionExpected){
            Assert.assertEquals(result, entry);
        }
        else{

            Assert.assertNull(result);
        }
    }

}