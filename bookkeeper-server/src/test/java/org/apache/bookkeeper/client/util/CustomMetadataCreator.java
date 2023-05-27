package org.apache.bookkeeper.client.util;

import java.util.HashMap;
import java.util.Map;

public class CustomMetadataCreator{

    private Map<String,byte[]> map ;

    public CustomMetadataCreator() {
        this.map = new HashMap<>();
    }

    public Map<String,byte[]> nullIstance(){
        return null;
    }
    public Map<String,byte[]> validIstance(){
        this.map = new HashMap<>();
        this.map.put("First_new_Metadata", "is only a test".getBytes());
        this.map.put("metadata", "2".getBytes());
        return map;
    }
    public Map<String,byte[]> nValidIstance(){
        this.map = new InvalidMap<>();
        this.map.put("a", "this non will be bytes".getBytes());
        return map;
    }

    public Map<String, byte[]> emptyIstance() {
        this.map = new HashMap<>();
        return map;
    }
}
