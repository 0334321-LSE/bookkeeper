package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    public Collection<Object[]> getParameters()  {
        //boundaryValues unidimensional selection
        return Arrays.asList(new Object[][] {
            //  ledgID                          DigestType          password      exception
    /*0*/   {this.ledgerHandle.getId(),         DigestType.MAC,     "abc",        false},

        });
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        this.bkClient = new BookKeeper(baseClientConf);
        this.ledgerHandle = this.bkClient.createLedger(3,2,1, BookKeeper.DigestType.DUMMY,"aaa".getBytes());
    }

    public BookKeeperOpenLedgerTest(long ledgerID, BookKeeper.DigestType digestType, byte[] password) {
        super(5, 180);
        this.ledgerID = ledgerID;
        this.digestType = digestType;
        this.password = password;
    }
}
