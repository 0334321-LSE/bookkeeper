package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.apache.bookkeeper.client.util.CustomMetadataCreator;
import org.apache.bookkeeper.client.util.CustomMetadataCreator.customMD;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@RunWith(Parameterized.class)
public class BookKeeperOpenLedgerTest extends
        BookKeeperClusterTestCase {
    private long ledgerID;
    private BookKeeper.DigestType digestType;
    private byte[] password;

    private Map<String,byte[]> customMetadata;
    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;

    private final CustomMetadataCreator mapCreator = new CustomMetadataCreator();
    private boolean isExceptionExpected;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
            //ledgID            DigestType          password          customMetadata        exception
    /*0*/   {1,                 DigestType.DUMMY,   "aaa".getBytes(), customMD.VALID,       false},
    /*1*/   {0,                 DigestType.DUMMY,   "aaa".getBytes(), customMD.VALID,       false},
    /*2*/   {0,                 DigestType.MAC,     "abc".getBytes(), customMD.VALID,       true},
    ///*3*/ {0,                 DigestType.MAC,     "aaa".getBytes(), customMD.VALID,       true},
             //Aggiunti successivamente
    /*4*/   {0,                 DigestType.DUMMY,   null,            customMD.VALID,       true},
    /*5*/   {0,                 DigestType.DUMMY,   "".getBytes(),   customMD.VALID,       true},


        });
    }

    @Before
    public void setUp() throws Exception {

        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        // creating a ledger
        this.bkClient = new BookKeeper(baseClientConf);

    }

    public BookKeeperOpenLedgerTest(long ledgerID, BookKeeper.DigestType digestType, byte[] password,customMD customParam, boolean isExceptionExpected)  {
        super(2, 70);
        this.ledgerID = ledgerID;
        this.digestType = digestType;
        this.password = password;
        this.isExceptionExpected = isExceptionExpected;
        switch (customParam){
            case NULL:
                this.customMetadata = this.mapCreator.nullInstance();
                break;
            case VALID:
                this.customMetadata = this.mapCreator.validInstance();
                break;
            case EMPTY:
                this.customMetadata = this.mapCreator.emptyInstance();
                break;
            case NOT_VALID:
                this.customMetadata = this.mapCreator.nValidInstance();
                break;
        }
    }

    @Test
    public void OpenLedgerTest() throws BKException, InterruptedException {
        this.ledgerHandle = this.bkClient.createLedger(2,2,1, BookKeeper.DigestType.DUMMY,"aaa".getBytes());

        if(this.isExceptionExpected){
            try {
                //exception was expected, it must go to catch branch
                LedgerHandle lh = this.bkClient.openLedger(this.ledgerID,this.digestType,this.password);
                Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            }catch (Exception e){
                System.out.println("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.");
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
                System.out.println("No exception was expected. Test is gone correctly");

            }catch (Exception e){
                Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                        this.isExceptionExpected);
            }
        }
    }

    //Added for PIT
    @Test
    public void OpenLedgerWithMetadataTest() throws BKException, InterruptedException {
        this.ledgerHandle = this.bkClient.createLedgerAdv(2,2,1, BookKeeper.DigestType.DUMMY,"aaa".getBytes(),this.customMetadata);

        if(this.isExceptionExpected){
            try {
                //exception was expected, it must go to catch branch
                LedgerHandle lh = this.bkClient.openLedger(this.ledgerID,this.digestType,this.password);
                Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            }catch (Exception e){
                System.out.println("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.");
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
                System.out.println("No exception was expected. Test is gone correctly");

            }catch (Exception e){
                Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                        this.isExceptionExpected);
            }
        }
    }
    @Override @After
    public void tearDown() throws Exception {
        if (this.ledgerHandle != null)
            this.ledgerHandle.close();
        if (this.bkClient != null)
            this.bkClient.close();
        if (zkc!=null)
            zkc.close();
    }

}
