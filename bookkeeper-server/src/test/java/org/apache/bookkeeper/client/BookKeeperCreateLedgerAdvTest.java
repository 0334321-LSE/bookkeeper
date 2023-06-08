package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.apache.bookkeeper.client.api.LedgerMetadata;
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
public class BookKeeperCreateLedgerAdvTest extends
        BookKeeperClusterTestCase {

    private static final long MINLONG = Long.MIN_VALUE;
    private static final long MAXLONG = Long.MAX_VALUE;

    // LedgerID must be >= 0
    private final long ledgerID;

    //The number of nodes the ledger is stored on
    private final int ensSize;

    //The number of nodes each entry is written to. In effect, the max replication for the entry.
    private final int wQS;
    //The number of nodes an entry must be acknowledged on. In effect, the minimum replication for the entry.
    private final int aQS;
    private final DigestType digestType;
    private final byte[] password;

    private Map<String,byte[]> customMetadata;

    private final boolean isExceptionExpected;

    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;

    private final CustomMetadataCreator mapCreator = new CustomMetadataCreator();

    private LedgerChecker checker;

    private enum customMD {
        NULL,
        VALID,
        NOT_VALID,
        EMPTY
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //For enS, wQS, aQS has been executed multidimensional selection, for the other unidimensional.
                //        lID      enS     wQS        aQS         digestType           passwd      customMetadata      exception

                 /*0*/{ 0,       1,      0,        -1,         DigestType.CRC32C,    "abc",      customMD.VALID,     true},
                 /*1*/{ 0,       1,      0,         0,         DigestType.DUMMY,     "abc",      customMD.VALID,     true},
                 /*2*/{ 0,       1,      0,         1,         DigestType.CRC32,     "abc",      customMD.VALID,     true},

                 /*3*/{ 0,       1,      1,         0,         DigestType.MAC,       "abc",      customMD.VALID,     false},
                 /*4*/{ 0,       1,      1,         1,         DigestType.CRC32C,    "abc",      customMD.VALID,     false},
                 /*5*/{ 0,       1,      1,         2,         DigestType.DUMMY,     "abc",      customMD.VALID,     true},

                 /*6*/{ 0,       1,      2,         1,         DigestType.CRC32,     "abc",      customMD.VALID,     true},
                 /*7*/{ 0,       1,      2,         2,         DigestType.MAC,       "abc",      customMD.VALID,     true},
                 /*8*/{ 0,       1,      2,         3,         DigestType.CRC32C,    "abc",      customMD.VALID,     true},

                 /*9*/{ 0,       0,     -1,       -2,          DigestType.MAC,       "abc",      customMD.VALID,     true},
                /*10*/{ 0,       0,     -1,       -1,          DigestType.CRC32C,    "abc",      customMD.VALID,     true},
                /*11*/{ 0,       0,     -1,        0,          DigestType.DUMMY,     "abc",      customMD.VALID,     true},

                /*12*/{ 0,       0,      0,       -1,          DigestType.CRC32,     "abc",      customMD.VALID,     true},
                /*13*/{ 0,       0,      0,        0,          DigestType.MAC,       "abc",      customMD.VALID,     true},
                /*14*/{ 0,       0,      0,        1,          DigestType.CRC32C,    "abc",      customMD.VALID,     true},

                /*15*/{ 0,       0,      1,        0,          DigestType.DUMMY,     "abc",      customMD.VALID,     true},
                /*16*/{ 0,       0,      1,        1,          DigestType.CRC32,     "abc",      customMD.VALID,     true},
                /*17*/{ 0,       0,      1,        2,          DigestType.MAC,       "abc",      customMD.VALID,     true},

                /*18*/{ 0,       -1,     -2,       -3,         DigestType.CRC32,     "abc",      customMD.VALID,     true},
                /*19*/{ 0,       -1,     -2,       -2,         DigestType.MAC,       "abc",      customMD.VALID,     true},
                /*20*/{ 0,       -1,     -2,       -1,         DigestType.CRC32C,    "abc",      customMD.VALID,     true},

                /*21*/{ 0,       -1,     -1,       -2,         DigestType.DUMMY,      "abc",     customMD.VALID,     true},
                /*22*/{ 0,       -1,     -1,       -1,         DigestType.CRC32,      "abc",     customMD.VALID,     true},
                /*23*/{ 0,       -1,     -1,        0,         DigestType.MAC,        "abc",     customMD.VALID,     true},

                /*24*/{ 0,       -1,     0,        -1,         DigestType.CRC32C,     "abc",     customMD.VALID,     true},
                /*25*/{ 0,       -1,     0,         0,         DigestType.DUMMY,      "abc",     customMD.VALID,     true},
                /*26*/{ 0,       -1,     0,         1,         DigestType.CRC32,      "abc",     customMD.VALID,     true},

                //null password doesn't cause exception
                /*27*/{ 0,        1,     1,         1,         DigestType.CRC32C,     null,      customMD.VALID,     true},

                //Those tests are especially written for CreateAdv, they test ledgerID and custom metadata
                /*28*/{ 0,       1,      1,         1,         DigestType.MAC,       "adv",      customMD.VALID,     false},
                /*29*/{MAXLONG,  1,      1,         1,         DigestType.CRC32C,    "adv",      customMD.VALID,     false},
                /*30*/{MINLONG,  1,      1,         1,         DigestType.MAC,       "adv",      customMD.VALID,     true},
                /*31*/{MAXLONG+1,1,      1,         1,         DigestType.CRC32C,    "adv",      customMD.VALID,     true},
                /*32*/{MINLONG+1,1,      1,         1,         DigestType.CRC32C,    "adv",      customMD.VALID,     true},
                /*33*/{ 0,       1,      1,         1,         DigestType.CRC32,     "adv",      customMD.EMPTY,     false},
                /*34*/{ 0,       1,      1,         1,         DigestType.DUMMY,     "adv",      customMD.NULL,      false},
                /*35*/{ 0,       1,      1,         1,         DigestType.DUMMY,     "adv",      customMD.NOT_VALID, true}
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

    public BookKeeperCreateLedgerAdvTest(long ledgerID, int ensSize, int wQS, int aQS, DigestType digestType, String passw, customMD customParam, boolean isExceptionExpected){
        super(3,60);
        this.ledgerID = ledgerID;

        this.ensSize = ensSize;
        this.wQS = wQS;
        this.aQS = aQS;
        this.digestType = digestType;
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
        if(passw != null)
            this.password = passw.getBytes();
        else
            this.password = null;
        this.isExceptionExpected =  isExceptionExpected;

    }

    @Test
    public void CreateLedgerAdvTest(){
        /*The main difference between this and createLedger is that
         Adv permits to set the ledgID and some customMetadata*/
        long entryId;
        if(this.isExceptionExpected){
            try {
                //exception was expected, it must go to catch branch
                this.ledgerHandle = this.bkClient.createLedgerAdv(this.ledgerID,this.ensSize,this.wQS,this.aQS,this.digestType,this.password,this.customMetadata);

                //TODO this block totally the execution
                entryId = this.ledgerHandle.addEntry("Expect and error".getBytes());

                /*if is null when here, can be considered a
                  right behavior (cause metadata are incorrect) */
                if(this.ledgerHandle == null)
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

                entryId = this.ledgerHandle.addEntry(0,"Expect that works".getBytes());
                entryId = this.ledgerHandle.addEntry(1,"Expect that works two times".getBytes());

                //Check if metadata are correct
                checkDataADV(this.ledgerHandle);

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

    private void checkData(LedgerHandle lh){
        LedgerMetadata metadata = ledgerHandle.getLedgerMetadata();

        Assert.assertEquals("ens size",metadata.getEnsembleSize(), this.ensSize);
        Assert.assertEquals("write quorum",metadata.getWriteQuorumSize(), this.wQS);
        Assert.assertEquals("ack quorum",metadata.getAckQuorumSize(), this.aQS);
        Assert.assertEquals("digest type",metadata.getDigestType().toString(), this.digestType.toString());

        if (this.password != null) {
            String pass1 = new String(this.password);
            String pass2 = new String(metadata.getPassword());
            Assert.assertEquals("password ", pass1, pass2);
        }

    }
    private void checkDataADV(LedgerHandle lh){
        LedgerMetadata metadata = ledgerHandle.getLedgerMetadata();
        checkData(lh);
        Assert.assertEquals("ledger id",metadata.getLedgerId(), this.ledgerID);
        if (this.customMetadata != null)
            Assert.assertEquals("custom metadata",metadata.getCustomMetadata(), this.customMetadata);
    }

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();
        //Close the ledger handler, bookkeeper client and the zookeeper
   /*     if (this.ledgerHandle != null)
            this.ledgerHandle.close();
        if (this.bkClient != null)
            this.bkClient.close();
        if (zkc!=null)
            zkc.close();*/
    }
}