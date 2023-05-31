package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;


import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.client.util.LedgerChecker;

import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
public class BookKeeperDeleteLedgerTest extends BookKeeperClusterTestCase {
    private final boolean isExceptionExpected;
    private final long ledgerID;
    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;

    private LedgerChecker checker;


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
                //ledgID            exception
        /*0*/   {0,                 false},
        /*1*/   {150,               true}
        });
    }
    @Before
    public void setUp() throws Exception{
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        this.checker = new LedgerChecker();
        // creating a ledger and adding dummy entry
        this.bkClient = new BookKeeper(baseClientConf);
        this.ledgerHandle = this.bkClient.createLedger(3,1,1, BookKeeper.DigestType.CRC32,"aaa".getBytes());
        this.ledgerHandle.addEntry("some bytes".getBytes());

    }

    public BookKeeperDeleteLedgerTest(long ledgerID, boolean expectedResult) {
        super(5, 60);
        this.ledgerID = ledgerID;
        this.isExceptionExpected = expectedResult;
    }

    @Test
    public void DeleteLedgerTest() throws Exception {
        if(this.isExceptionExpected){
            /*TODO ask prof wich kind of behavior we must expect, actually
               the ledger is still alive but it doesn't catch any exception*/
            //try {
                this.bkClient.deleteLedger(this.ledgerID);
                //Check if it still exits
                if (!checker.check(this.bkClient, this.ledgerHandle.getId()))
                    Assert.fail("Ledger id wrong but still deleted");
                else
                    Assert.assertTrue("Ledger is still alive", true);

                //   Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

            /*}catch (Exception e){
                Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                        this.isExceptionExpected);
            }*/
        }else{
            try {
                long deletedID = this.ledgerHandle.getId();
                //exception wasn't expected, it must remain here
                this.bkClient.deleteLedger(deletedID);
                //Check if it still exits
                if (this.checker.check(this.bkClient,deletedID))
                    Assert.fail("Ledger id was right but not deleted");
                System.out.println("\n!!! Test gone correctly, no exception was found.");
                Assert.assertFalse("No exception was expected. Ledger correctly deleted", this.isExceptionExpected);
            }catch (Exception e){
                System.out.println("\n!!! Caught exception: --->"+e.getClass().getName());
                Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                        this.isExceptionExpected);
            }
        }
    }

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (this.bkClient != null)
            this.bkClient.close();
        if (this.zkc!=null)
            this.zkc.close();


    }



}
