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
public class WriteCacheGetTest {

    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;
    private ByteBuf entry;
    private final long ledgerId = 2;
    private final long entryId = 1;
    private final boolean isValidLedgerId;
    private final boolean isValidEntryId;

    //True as positive result, false as negative
    private final boolean expectedResult;


    @Before
    public void setUp() {
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        int entryNumber = 10;
        int entrySize = 1024;
        this.writeCache = new WriteCache(this.byteBufAllocator, entrySize * entryNumber);

        this.entry = byteBufAllocator.buffer(entrySize);

        this.entry.writeBytes("bytes into the entry".getBytes());

        this.writeCache.put(ledgerId, entryId, entry);
    }

    @After
    public void tearDown() throws Exception {
        this.writeCache.clear();
        this.entry.release();
        this.writeCache.close();
    }

    //Input parameters
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {
                // ledgerID     entryID     result
                {  true,        true,       true},
                {  true,        false,      false},
                {  false,       true,       false},
                {  false,       false,      false}
        });
    }

    public WriteCacheGetTest(boolean isLedgerValid, boolean isEntryValid , boolean expectedResult){
        this.isValidLedgerId = isLedgerValid;
        this.isValidEntryId = isEntryValid;
        this.expectedResult = expectedResult;
    }

    @Test
    public void getFromCacheTest(){

        ByteBuf result = null;

        long  actualLedgerId = ledgerId, actualEntryId = entryId;

        if(!this.isValidLedgerId) actualLedgerId++;
        if(!this.isValidEntryId) actualEntryId++;


        System.out.println("validLedgerID: "+this.ledgerId  + "\t|\t actualLedgerID: "+actualLedgerId);
        System.out.println("validEntryID:  "+ this.entryId  + "\t|\t actualEntryID:  "+actualEntryId);
        System.out.println("----------------------------------------");
        result = writeCache.get(actualLedgerId, actualEntryId);


        if(this.expectedResult){
            Assert.assertEquals(result, entry);
        }
        else{
            Assert.assertNull(result);
        }
    }

}