package org.apache.bookkeeper.client;

public class BookKeeperAdminTest extends BookKeeperClusterTestCase {
    private boolean expectedResult;
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final int numOfBookies = 2;

    private boolean hasValidServConf;
    private boolean isInteractive;
    private boolean isInteractiveYes;
    private boolean force;

    public BookKeeperAdminTest(boolean hasValidServConf, boolean isInteractive, boolean isInteractiveYes, boolean force, boolean expectedResult) {

        super(numOfBookies);
        this.hasValidServConf = hasValidServConf;
        this.isInteractive = isInteractive;
        this.isInteractiveYes = isInteractiveYes;
        this.force = force;
        this.expectedResult = expectedResult;

    }
}
