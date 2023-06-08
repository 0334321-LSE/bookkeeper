package org.apache.bookkeeper.client.util;

import java.util.HashMap;
import java.util.Map;



public class CustomMetadataCreator{
    public enum customMD {
        NULL,
        VALID,
        NOT_VALID,
        EMPTY
    }

    private Map<String,byte[]> map ;

    public CustomMetadataCreator() {
        this.map = new HashMap<>();
    }

    public Map<String,byte[]> nullInstance(){
        return null;
    }
    public Map<String,byte[]> validInstance(){
        //TODO SEEMS THAT THIS CREATE PROBLEM, CLIENT DOESN'T SEE LEDGER WHEN I
        // PASS A VALID INSTANCE
        this.map = new HashMap<>();
        this.map.put("metadata1", "1".getBytes());
        this.map.put("metadata2", "2".getBytes());
        return map;
    }
    public Map<String,byte[]> nValidInstance(){
        this.map = new InvalidMap<>();
        this.map.put("a", "this non will be bytes".getBytes());
        return map;
    }

    public Map<String, byte[]> emptyInstance() {
        this.map = new HashMap<>();
        return map;
    }
}
