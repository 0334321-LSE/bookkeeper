package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookKeeperOpenLedgerTest extends
        BookKeeperClusterTestCase {
    private long ledgerID;
    private BookKeeper.DigestType digestType;
    private byte[] password;
    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;
    private boolean isExceptionExpected;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
            //ledgID            DigestType          password                 exception
    /*0*/   {1,                 DigestType.DUMMY,   "aaa".getBytes(),        false},
    /*1*/   {0,                 DigestType.MAC,     "abc".getBytes(),        true},
    /*2*/   {0,                 DigestType.CRC32C,   null,                   true}
        });
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        // creating a ledger
        this.bkClient = new BookKeeper(baseClientConf);
        this.ledgerHandle = this.bkClient.createLedger(3,2,1, BookKeeper.DigestType.DUMMY,"aaa".getBytes());

    }

    public BookKeeperOpenLedgerTest(long ledgerID, BookKeeper.DigestType digestType, byte[] password, boolean isExceptionExpected) throws BKException, IOException, InterruptedException {
        super(5, 180);
        this.ledgerID = ledgerID;
        this.digestType = digestType;
        this.password = password;
        this.isExceptionExpected = isExceptionExpected;
    }

    @Test
    public void OpenLedgerTest() {

        if(this.isExceptionExpected){
            try {
                //exception was expected, it must go to catch branch
                LedgerHandle lh = this.bkClient.openLedger(this.ledgerID,this.digestType,this.password);
                Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            }catch (Exception e){
                Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                        this.isExceptionExpected);
            }
        }else{
            try {
                //exception wasn't expected, it must remain here
                LedgerHandle lh = this.bkClient.openLedger(this.ledgerHandle.getId(),this.digestType,this.password);
                //Must be not null
                if(null == lh) Assert.assertNotNull(lh);
                Assert.assertFalse("No exception was expected. Test is gone correctly", this.isExceptionExpected);

            }catch (Exception e){
                Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                        this.isExceptionExpected);
            }
        }
    }

    @After
    public void tearDown() throws BKException, InterruptedException {
        if (this.ledgerHandle != null)
            this.ledgerHandle.close();
        if (this.bkClient != null)
            this.bkClient.close();
        if (zkc!=null)
            zkc.close();
    }

}
