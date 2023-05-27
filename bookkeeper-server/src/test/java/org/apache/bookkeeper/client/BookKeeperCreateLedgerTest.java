package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.apache.bookkeeper.client.util.CustomMetadataCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.bookkeeper.client.util.LedgerChecker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;


@RunWith(Parameterized.class)
public class BookKeeperCreateLedgerTest extends
        BookKeeperClusterTestCase {

    private static final int MAXINT = Integer.MAX_VALUE;
    private static final int MININT = Integer.MIN_VALUE;
    private static final long MINLONG = Long.MIN_VALUE;
    private static final long MAXLONG = Long.MIN_VALUE;

    private final long ledgerID;
    private final int ensSize;
    private final int wQS;
    private final int aQS;
    private final DigestType digestType;
    private final byte[] password;

    private Map<String,byte[]> customMetadata;

    private final boolean isExceptionExpected;

    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;
    private CustomMetadataCreator mapCreator = new CustomMetadataCreator();

    private LedgerChecker checker;

    private enum customMD {
        NULL,
        VALID,
        NOT_VALID,
        EMPTY
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
            //  LedgID     En_Size     wQS         aQS        DigestType             password     customMetadata      exception
                // each boundary values of eSize, wQS, aQS, Digest and password
        /*0*/   {1,         4,         3,          2,         DigestType.MAC,        "abc",       customMD.VALID,     false},
        /*1*/   {0,         3,         2,          1,         DigestType.CRC32,      "abc",       customMD.VALID,     false},
        /*2*/   {-1,        2,         1,          0,         DigestType.CRC32C,     "",          customMD.VALID,     false},
                // wQS = aQS = 0 no replication-> expected exception ?
        /*3*/   {1,         1,         0,          0,         DigestType.DUMMY,      "abc",       customMD.VALID,     true},
                // eSize = wQs = aQS = 0 no replication -> expected exception ?
        /*4*/   {1,         0,         0,          0,         DigestType.DUMMY,      "abc",       customMD.VALID,     true},
                // eSize < wQs && eSize < 0 -> expected exception
        /*5*/   {1,         -1,        2,          1,         DigestType.CRC32C,     "abc",       customMD.VALID,     true},
                // eSize < wQs < aQs && eSize < 0  -> expected exception
        /*6*/   {1,         -2,        1,          2,         DigestType.MAC,        "abc",       customMD.VALID,     true},
                // eSize < wQs && eSize < 0 -> expected exception
        /*7*/   {1,         -3,        1,          1,         DigestType.DUMMY,      "abc",       customMD.VALID,     true},
                // wQS = aQS < 0 no sense -> expected exception
        /*8*/   {1,         1,         -1,         -1,        DigestType.MAC,        "abc",       customMD.VALID,     true},
                // wQS < 0 && wQS <aQS -> expected exception
        /*9*/   {1,         3,         -2,         0,         DigestType.MAC,        "abc",       customMD.VALID,     true},
                // null password -> expected exception
        /*10*/   {1,        2,         2,          1,         DigestType.MAC,        null,        customMD.VALID,     true},
                // special characters composed password-> no exception
        /*11*/   {1,        3,         2,          1,         DigestType.MAC,        "/n /t",     customMD.VALID,     false},
                //Those test are especially written for CreateAdv
                // maxLong as ledgerID -> no exception
        /*12*/   {MAXLONG,  3,         2,          1,         DigestType.MAC,        "maxInt",    customMD.VALID,     false},
                // minLong as ledgerID -> no exception
        /*13*/   {MINLONG,  2,         2,          1,         DigestType.MAC,        "minInt",    customMD.VALID,     false},
                // maxLong+1 as ledgerID ->  exception
        /*14*/   {MAXLONG+1,  3,         2,          1,       DigestType.MAC,        "maxInt",    customMD.VALID,     true},
                // minLong+1 as ledgerID ->  exception
        /*15*/   {MINLONG+1,  2,         2,          1,       DigestType.MAC,        "minInt",    customMD.VALID,     true},
                // customMetadata empty -> no exception
        /*16*/   {1,          3,         2,          1,       DigestType.MAC,        "abc",       customMD.EMPTY,     false},
                // customMetadata null  -> no exception(?)
        /*17*/   {1,          2,         2,          1,       DigestType.MAC,        "cde",       customMD.NULL,      false},
                // customMetadata NOT VALID  -> should exception
        /*18*/   {1,          2,         2,          1,       DigestType.MAC,        "fgh",       customMD.NOT_VALID, true},

                //TODO: ask deAngelis how to work with test parameters with ID : 3-4-8
        });
    }

    @Before
    public void setUp() throws Exception {

        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        this.bkClient = new BookKeeper(baseClientConf);
        this.checker = new LedgerChecker();
    }

    public BookKeeperCreateLedgerTest(long ledgerID, int ensSize, int wQS, int aQS, DigestType digestType, String passw, customMD customParam, boolean isExceptionExpected){
        super(5,60);
        this.ledgerID = ledgerID;

        this.ensSize = ensSize;
        this.wQS = wQS;
        this.aQS = aQS;
        this.digestType = digestType;
        switch (customParam){
            case NULL:
                this.customMetadata = this.mapCreator.nullIstance();
            case VALID:
                this.customMetadata = this.mapCreator.validIstance();
            case EMPTY:
                this.customMetadata = this.mapCreator.emptyIstance();
            case NOT_VALID:
                this.customMetadata = this.mapCreator.nValidIstance();
        }
        if(passw != null)
            this.password = passw.getBytes();
        else
            this.password = null;
        this.isExceptionExpected =  isExceptionExpected;

    }

    @Test
    public void CreateLedgerTest() {

        long entryId;
        if(this.isExceptionExpected){
                try {
                    //exception was expected, it must go to catch branch
                    this.ledgerHandle = this.bkClient.createLedger(this.ensSize,this.wQS,this.aQS,this.digestType,this.password);

                    //TODO this block totally the execution
                    //entryId = this.ledgerHandle.addEntry("Expect and error".getBytes());

                    if(this.ledgerHandle == null)
                        //if is null when here, can be considered a right behavior
                        Assert.assertNull(this.ledgerHandle);
                    Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

                }catch (Exception e){
                    Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                    this.isExceptionExpected);
                 }
            }else{
                try {
                    //exception wasn't expected, it must remain here
                    this.ledgerHandle = this.bkClient.createLedger(this.ensSize,this.wQS,this.aQS,this.digestType,this.password);
                    if(this.ledgerHandle == null)
                        //must be not null
                        Assert.assertNotNull(this.ledgerHandle);
                    entryId = this.ledgerHandle.addEntry("Expect that works".getBytes());

                    Assert.assertFalse("No exception was expected. Test is gone correctly", this.isExceptionExpected);

                }catch (Exception e){
                    Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                            this.isExceptionExpected);
                }
            }
    }

    @Test
    public void CreateLedgerAdvTest(){
        /*The main difference between this and the previous method is that
         Adv permits to set the ledgID and some customMetadata*/
        long entryId;
        if(this.isExceptionExpected){
            try {
                //exception was expected, it must go to catch branch
                this.ledgerHandle = this.bkClient.createLedgerAdv(this.ledgerID,this.ensSize,this.wQS,this.aQS,this.digestType,this.password,this.customMetadata);

                //TODO this block totally the execution
                //entryId = this.ledgerHandle.addEntry("Expect and error".getBytes());

                if(this.ledgerHandle == null)
                    //if is null when here, can be considered a right behavior
                    Assert.assertNull(this.ledgerHandle);
                Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            }catch (Exception e){
                Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                        this.isExceptionExpected);
            }
        }else{
            try {
                //exception wasn't expected, it must remain here
                this.ledgerHandle = this.bkClient.createLedgerAdv(this.ledgerID,this.ensSize,this.wQS,this.aQS,this.digestType,this.password,this.customMetadata);

                //must be not null
                if(this.ledgerHandle == null)
                    Assert.assertNotNull(this.ledgerHandle);

                entryId = this.ledgerHandle.addEntry("Expect that works".getBytes());

                //Check if the ledger was correctly added
                if (!this.checker.check(this.bkClient,this.ledgerID))
                    Assert.fail("Ledger ID isn't here");

                Assert.assertFalse("No exception was expected. Test is gone correctly", this.isExceptionExpected);

            }catch (Exception e){
                Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                        this.isExceptionExpected);
            }
        }

    }



    @After
    public void tearDown() throws BKException, InterruptedException {
        //Close the ledger handler, bookkeeper client and the zookeeper
        if (this.ledgerHandle != null)
            this.ledgerHandle.close();
        if (this.bkClient != null)
            this.bkClient.close();
        if (zkc!=null)
            zkc.close();
    }
}