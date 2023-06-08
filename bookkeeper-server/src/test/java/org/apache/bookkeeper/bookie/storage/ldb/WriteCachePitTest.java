package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.util.InvalidByteBuf;
import org.apache.bookkeeper.bookie.storage.ldb.util.InvalidByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)

public class WriteCachePitTest {
    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;
    private ByteBuf entry;
    private int entrySize;
    private long maxCacheSize;
    private int maxSegmentSize;

    private boolean isExpectedException;

    @Before
    public void setUp(){
        this.entry = UnpooledByteBufAllocator.DEFAULT.buffer(16);
        this.entry.writeBytes("Something".getBytes());

    }

    public WriteCachePitTest(ByteBufAllocator byteBufAllocator, long maxCacheSize, int maxSegmentSize, boolean isExpectedException) {
        this.byteBufAllocator = byteBufAllocator;
        this.maxCacheSize = maxCacheSize;
        this.maxSegmentSize = maxSegmentSize;
        this.isExpectedException = isExpectedException;

    }

    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        /*Some case are commented because the constructor:
        1) doesn't check if the byteBufAllocator is implemented right,
        it can be a right/passable behavior not necessary is a constructor responsibility
        2) admits maxCache size < maxSegmentSize and for me this is not a right behavior*/

        return Arrays.asList(new Object[][] {
                // byteBufAllocator     maxCacheSize     maxSegmentSize     isExpectedException
///*0*/{  UnpooledByteBufAllocator.DEFAULT,                1,            2,        true},
/*1*/{  UnpooledByteBufAllocator.DEFAULT,                1,            1,        false},
/*2*/{  UnpooledByteBufAllocator.DEFAULT,                1,            0,        true},

///*3*/{  UnpooledByteBufAllocator.DEFAULT,                0,            1,        true},
/*4*/{  UnpooledByteBufAllocator.DEFAULT,                0,            0,        true},
/*5*/{  UnpooledByteBufAllocator.DEFAULT,                0,           -1,        true},

/*6*/{  UnpooledByteBufAllocator.DEFAULT,               -1,            0,        true},
/*7*/{  UnpooledByteBufAllocator.DEFAULT,               -1,           -1,        true},
/*8*/{  UnpooledByteBufAllocator.DEFAULT,               -1,           -2,        true},

                   //    /*9*/{  null,                1,            2,        true},
                   //   /*10*/{  null,                1,            1,        true},
                      /*11*/{  null,                1,            0,        true},

                   //   /*12*/{  null,                0,            1,        true},
                      /*13*/{  null,                0,            0,        true},
                      /*14*/{  null,                0,           -1,        true},

                      /*15*/{  null,                -1,            0,       true},
                      /*16*/{  null,                -1,           -1,       true},
                      /*17*/{  null,                -1,           -2,       true},

///*18*/{  new InvalidByteBufAllocator(),                1,            2,        true},
///*19*/{  new InvalidByteBufAllocator(),                1,            1,        true},
/*20*/{  new InvalidByteBufAllocator(),                1,            0,        true},

///*21*/{  new InvalidByteBufAllocator(),                0,            1,        true},
/*22*/{  new InvalidByteBufAllocator(),                0,            0,        true},
/*23*/{  new InvalidByteBufAllocator(),                0,           -1,        true},

/*24*/{  new InvalidByteBufAllocator(),               -1,            0,        true},
/*25*/{  new InvalidByteBufAllocator(),               -1,           -1,        true},
/*26*/{  new InvalidByteBufAllocator(),               -1,           -2,        true},
        });
    }

    @Test
    public void ConstructorTest(){
        try{
            this.writeCache = new WriteCache(this.byteBufAllocator,this.maxCacheSize,this.maxSegmentSize);
            this.writeCache.put(0,0,this.entry);
            Assert.assertFalse(this.isExpectedException);
        }catch (Exception e){
            Assert.assertTrue(this.isExpectedException);
        }
    }
}
