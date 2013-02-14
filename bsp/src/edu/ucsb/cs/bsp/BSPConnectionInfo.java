package edu.ucsb.cs.bsp;

import java.util.ArrayList;
import java.util.List;

public class BSPConnectionInfo {

    private static final BSPConnectionInfo instance = new BSPConnectionInfo();

    private List<String> connectionInfo = new ArrayList<String>();

    private BSPConnectionInfo() {

    }

    public static BSPConnectionInfo getInstance() {
        return instance;
    }

    public void add(String info) {
        connectionInfo.add(info);
    }

    public String[] getConnectionInfo() {
        return connectionInfo.toArray(new String[connectionInfo.size()]);
    }

}
