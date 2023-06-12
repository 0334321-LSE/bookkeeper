package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookKeeperCreateLedgerPitTest extends
        BookKeeperClusterTestCase {


    //The number of nodes the ledger is stored on
    private final int ensSize;

    //The number of nodes each entry is written to. In effect, the max replication for the entry.
    private final int wQS;
    //The number of nodes an entry must be acknowledged on. In effect, the minimum replication for the entry.
    private final int aQS;
    private final BookKeeper.DigestType digestType;
    private final byte[] password;

    private final boolean isExceptionExpected;

    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //For enS, wQS, aQS has been executed multidimensional selection, for the other unidimensional.
                //         enS     wQS        aQS         digestType           passwd         exception
                /*0*/{      1,      1,         0,         DigestType.MAC,       "abc",         false},
                /*1*/{      1,      1,         1,         DigestType.CRC32C,    "abc",         false},
        });
    }

    @Before
    public void setUp() throws Exception {

        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        //Set opportunisticStriping to true
        baseClientConf.setOpportunisticStriping(true);
        super.setUp();
        this.bkClient = new BookKeeper(baseClientConf);

    }

    public BookKeeperCreateLedgerPitTest(int ensSize, int wQS, int aQS, DigestType digestType, String passw, boolean isExceptionExpected) {
        super(2, 60);

        this.ensSize = ensSize;
        this.wQS = wQS;
        this.aQS = aQS;
        this.digestType = digestType;

        if (passw != null)
            this.password = passw.getBytes();
        else
            this.password = null;
        this.isExceptionExpected = isExceptionExpected;

    }

    @Test
    public void CreateLedgerPitTest() {
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
                System.out.println("\n!!! Caught exception: --->" + e.getClass().getName());
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

    private void checkData(LedgerHandle lh) {
        LedgerMetadata metadata = ledgerHandle.getLedgerMetadata();

        Assert.assertEquals("ens size", metadata.getEnsembleSize(), this.ensSize);
        Assert.assertEquals("write quorum", metadata.getWriteQuorumSize(), this.wQS);
        Assert.assertEquals("ack quorum", metadata.getAckQuorumSize(), this.aQS);
        Assert.assertEquals("digest type", metadata.getDigestType().toString(), this.digestType.toString());

        if (this.password != null) {
            String pass1 = new String(this.password);
            String pass2 = new String(metadata.getPassword());
            Assert.assertEquals("password ", pass1, pass2);
        }

    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

    }

}