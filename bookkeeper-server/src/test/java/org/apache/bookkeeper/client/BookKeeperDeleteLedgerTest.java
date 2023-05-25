package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookKeeperDeleteLedgerTest extends BookKeeperClusterTestCase {
    private boolean isExceptionExpected;
    private long ledgerID;
    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
                //ledgID            exception
        /*0*/   {1,                 false},
        /*1*/   {0,                 true}
        });
    }


    public BookKeeperDeleteLedgerTest(long ledgerID, boolean expectedResult) {
        super(5, 180);
        this.ledgerID = ledgerID;
    }
    @Before
    public void setUp() throws Exception{
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        // creating a ledger
        this.bkClient = new BookKeeper(baseClientConf);
        this.ledgerHandle = this.bkClient.createLedger(3,2,1, BookKeeper.DigestType.DUMMY,"aaa".getBytes());
    }

    @Test
    public void DeleteLedgerTest(){

        if(this.isExceptionExpected){
            try {
                //exception was expected, it must go to catch branch
                this.bkClient.deleteLedger(this.ledgerID);
                Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            }catch (Exception e){
                Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                        this.isExceptionExpected);
            }
        }else{
            try {
                //exception wasn't expected, it must remain here
                this.bkClient.deleteLedger(this.ledgerHandle.getId());
                Assert.assertFalse("No exception was expected. Test is gone correctly", this.isExceptionExpected);

            }catch (Exception e){
                Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                        this.isExceptionExpected);
            }
        }
    }
}
