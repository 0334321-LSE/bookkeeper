package org.apache.bookkeeper.client.BookKeeperTests;


import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
public class BookKeeperCreateLedgerTest extends
        BookKeeperClusterTestCase{

    private int ensSize;
    private int wQS;
    private int aQS;
    private DigestType digestType;
    private byte[] password;
    private boolean isExceptionExpected;

    private BookKeeper bkClient;
    private LedgerHandle ledgerHandle;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
            //  En_Size     wQS         aQS        DigestType             password      exception
                // each boundary values of eSize, wQS, aQS, Digest and password
        /*0*/   {4,         3,          2,         DigestType.MAC,        "abc",        false},
        /*1*/   {3,         2,          1,         DigestType.CRC32,      "abc",        false},
        /*2*/   {2,         1,          0,         DigestType.CRC32C,     "",           false},
                // wQS = aQS = 0 no replication-> expected exception
        /*3*/   {1,         0,          0,         DigestType.DUMMY,      "abc",        true},
                // eSize = wQs = aQS = 0 no replication -> expected exception
        /*4*/   {0,         0,          0,         DigestType.DUMMY,      "abc",        true},
                // eSize < wQs && eSize < 0 -> expected exception
        /*5*/   {-1,         2,          1,        DigestType.CRC32C,     "abc",        true},
                // eSize < wQs < aQs && eSize < 0  -> expected exception
        /*6*/   {-2,         1,          2,        DigestType.MAC,        "abc",        true},
                // eSize < wQs && eSize < 0 -> expected exception
        /*7*/   {-3,         1,          1,        DigestType.DUMMY,      "abc",        true},
                // wQS = aQS < 0 no sense -> expected exception
        /*8*/   {1,         -1,          -1,       DigestType.MAC,        "abc",        true},
                // wQS < 0 && wQS <aQS -> expected exception
        /*9*/   {4,         -2,          0,        DigestType.MAC,        "abc",        true},
                // null password -> expected exception
        /*10*/   {4,         2,          1,        DigestType.MAC,        null,        true},
                // special characters composed password-> no exception
        /*11*/   {4,         2,          1,        DigestType.MAC,        "/n /t",        false},


                //TODO: ask deAngelis how to work with test parameters with ID : 3-4-8

        });
    }

    @Before
    public void setUp() throws Exception {

        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        this.bkClient = new BookKeeper(baseClientConf);
    }

    public BookKeeperCreateLedgerTest(int ensSize, int wQS, int aQS, DigestType digestType, String passw, boolean isExceptionExpected){
        super(5,180);

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

            if(this.isExceptionExpected){
                try {
                    //exception was expected, it must go to catch branch
                    this.ledgerHandle = this.bkClient.createLedger(this.ensSize,this.wQS,this.aQS,this.digestType,this.password);
                    Assert.assertFalse("An exception was expected. Test is gone wrong", this.isExceptionExpected);

                }catch (Exception e){
                    Assert.assertTrue("An exception was expected. Test is gone right: " + e.getClass().getName() + " has been thrown.",
                    this.isExceptionExpected);
                 }
            }else{
                try {
                    //exception wasn't expected, it must remain here
                    this.ledgerHandle = this.bkClient.createLedger(this.ensSize,this.wQS,this.aQS,this.digestType,this.password);
                    //Must be not null
                    Assert.assertNotNull(this.ledgerHandle);
                    Assert.assertFalse("No exception was expected. Test is gone correctly", this.isExceptionExpected);

                }catch (Exception e){
                    Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown. Test is gone wrong",
                            this.isExceptionExpected);
                }
            }


    }



    @After
    public void tearDown() throws BKException, InterruptedException, IOException {
        if (this.ledgerHandle != null)
            this.ledgerHandle.close();
    }
}