package org.apache.bookkeeper.client.util;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;

import java.util.ArrayList;


public class LedgerChecker {

    private Iterable<Long> ledgerIds;
    private ArrayList<Long> arrayLedgerId;

    public LedgerChecker() {
    }

    public boolean check(BookKeeper bk, long lID) throws Exception {

        // BookKeeperAdmin to obtain info on the ledger
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bk);

        arrayLedgerId = new ArrayList<>();

        // Return list of ledger in the cluster
        ledgerIds =  bkAdmin.listLedgers();

        toArray(ledgerIds,arrayLedgerId);

        // Check if lID is in the list
        for (long element : arrayLedgerId) {
            if(lID == element)
                return true;
        }

        // Chiudi la connessione al cluster BookKeeper
        bkAdmin.close();
        return false;

    }
    private void toArray(Iterable<Long> iterable, ArrayList<Long> list){
       for( long iterate : iterable){
           list.add(iterate);
        }
    }
}
