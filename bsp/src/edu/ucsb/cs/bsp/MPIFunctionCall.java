package edu.ucsb.cs.bsp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class MPIFunctionCall {

    private String functionName;
    private Map<String,String> arguments = new HashMap<String, String>();

    public MPIFunctionCall(BufferedReader reader) throws IOException {
        this.functionName = reader.readLine();
        while (this.functionName != null) {
            String line = reader.readLine();
            if (line == null || "".equals(line)) {
                break;
            } else {
                int index = line.indexOf('=');
                String key = line.substring(0, index);
                String value = line.substring(index + 1);
                arguments.put(key, value);
            }
        }
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getArgument(String name) {
        return arguments.get(name);
    }
}
