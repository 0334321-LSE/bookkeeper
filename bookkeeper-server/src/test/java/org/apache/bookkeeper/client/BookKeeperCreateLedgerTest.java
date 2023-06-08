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
public class BookKeeperCreateLedgerTest extends
        BookKeeperClusterTestCase {


    //The number of nodes the ledger is stored on
    private final int ensSize;

    //The number of nodes each entry is written to. In effect, the max replication for the entry.
    private final int wQS;
    //The number of nodes an entry must be acknowledged on. In effect, the minimum replication for the entry.
    private final int aQS;
    private final DigestType digestType;
    private final byte[] password;

    private final boolean isExceptionExpected;

    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
         //For enS, wQS, aQS has been executed multidimensional selection, for the other unidimensional.
         //              enS     wQS        aQS         digestType           passwd         exception

           // /*0*/{        1,      0,        -1,         DigestType.CRC32C,    "abc",           true},
           // /*1*/{        1,      0,         0,         DigestType.DUMMY,     "abc",           true},
            /*2*/{        1,      0,         1,         DigestType.CRC32,     "abc",           true},

            /*3*/{        1,      1,         0,         DigestType.MAC,       "abc",           false},
            /*4*/{        1,      1,         1,         DigestType.CRC32C,    "abc",           false},
            /*5*/{        1,      1,         2,         DigestType.DUMMY,     "abc",           true},

         //   /*6*/{        1,      2,         1,         DigestType.CRC32,     "abc",           true},
         //   /*7*/{        1,      2,         2,         DigestType.MAC,       "abc",           true},
            /*8*/{        1,      2,         3,         DigestType.CRC32C,    "abc",           true},

         //   /*9*/{        0,     -1,       -2,          DigestType.MAC,       "abc",           true},
         //  /*10*/{        0,     -1,       -1,          DigestType.CRC32C,    "abc",           true},
           /*11*/{        0,     -1,        0,          DigestType.DUMMY,     "abc",           true},

         //  /*12*/{        0,      0,       -1,          DigestType.CRC32,     "abc",           true},
         //  /*13*/{        0,      0,        0,          DigestType.MAC,       "abc",           true},
           /*14*/{        0,      0,        1,          DigestType.CRC32C,    "abc",           true},

         //  /*15*/{        0,      1,        0,          DigestType.DUMMY,     "abc",           true},
         //  /*16*/{        0,      1,        1,          DigestType.CRC32,     "abc",           true},
           /*17*/{        0,      1,        2,          DigestType.MAC,       "abc",           true},

           /*18*/{        -1,     -2,       -3,         DigestType.CRC32,     "abc",           true},
           /*19*/{        -1,     -2,       -2,         DigestType.MAC,       "abc",           true},
           /*20*/{        -1,     -2,       -1,         DigestType.CRC32C,    "abc",           true},

           /*21*/{        -1,     -1,       -2,         DigestType.DUMMY,      "abc",          true},
           /*22*/{        -1,     -1,       -1,         DigestType.CRC32,      "abc",          true},
           /*23*/{        -1,     -1,        0,         DigestType.MAC,        "abc",          true},

           /*24*/{        -1,     0,        -1,         DigestType.CRC32C,     "abc",          true},
           /*25*/{        -1,     0,         0,         DigestType.DUMMY,      "abc",          true},
           /*26*/{        -1,     0,         1,         DigestType.CRC32,      "abc",          true},

                //null password doesn't cause exception
           /*27*/{         1,     1,         1,         DigestType.CRC32C,     null,           true},

        });
    }

    @Before
    public void setUp() throws Exception {

        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        this.bkClient = new BookKeeper(baseClientConf);

    }

    public BookKeeperCreateLedgerTest( int ensSize, int wQS, int aQS, DigestType digestType, String passw, boolean isExceptionExpected){
        super(3,60);


        this.ensSize = ensSize;
        this.wQS = wQS;
        this.aQS = aQS;
        this.digestType = digestType;

        if(passw != null)
            this.password = passw.getBytes();
        else
            this.password = null;
        this.isExceptionExpected =  isExceptionExpected;

    }

    @Test
    public void CreateLedgerTest() {
        long entryId;

        if (this.isExceptionExpected) {
            try {
                //exception was expected, it must go to catch branch
                this.ledgerHandle = this.bkClient.createLedger(this.ensSize, this.wQS, this.aQS, this.digestType, this.password);

                //TODO doesn't permit to write when wQS = 0 (right) but doesn't catch exception.
                entryId = this.ledgerHandle.addEntry("Expect and error".getBytes());

                if (this.ledgerHandle == null)
                    //if is null when here, can be considered a right behavior
                    Assert.assertNull(this.ledgerHandle);
                Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            } catch (Exception e) {
                System.out.println("\n!!! Caught exception: --->"+e.getClass().getName());
                Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                        this.isExceptionExpected);
            }
        } else {
            try {
                //exception wasn't expected, it must remain here
                this.ledgerHandle = this.bkClient.createLedger(this.ensSize, this.wQS, this.aQS, this.digestType, this.password);

                if (this.ledgerHandle == null)
                    //must be not null
                    Assert.assertNotNull(this.ledgerHandle);

                entryId = this.ledgerHandle.addEntry("Expect that works".getBytes());
                entryId = this.ledgerHandle.addEntry("Expect that works two times".getBytes());

                //check if date are correct
                checkData(this.ledgerHandle);

                Assert.assertFalse("No exception was expected. Test is gone correctly", this.isExceptionExpected);

            } catch (Exception e) {
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

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();
/*        //Close the ledger handler, bookkeeper client and the zookeeper
        if (this.ledgerHandle != null)
            this.ledgerHandle.close();
        if (this.bkClient != null)
            this.bkClient.close();
        if (zkc!=null)
            zkc.close();*/
    }
}