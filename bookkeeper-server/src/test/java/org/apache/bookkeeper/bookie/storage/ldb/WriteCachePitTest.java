package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)

public class WriteCachePitTest {
    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;
    private ByteBuf entry;
    private int entrySize;
    private int maxSegmentSize;

    //TODO complete this test on constructor method.
    public WriteCachePitTest() {
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
    }

    @Test
    public void ConstructorTest(){
        this.writeCache = new WriteCache(this.byteBufAllocator, this.entrySize);
    }
}
