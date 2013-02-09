package edu.ucsb.cs.bsp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class MPIFunctionData {

    private String functionName;
    private Map<String,String> arguments = new HashMap<String, String>();
    private Map<String,String[]> buffers = new HashMap<String, String[]>();

    public MPIFunctionData(BufferedReader reader) throws IOException {
        this.functionName = reader.readLine();
        List<String> bufferKeys = new ArrayList<String>();
        while (true) {
            String line = reader.readLine();
            if ("".equals(line)) {
                break;
            } else {
                int index = line.indexOf('=');
                String key = line.substring(0, index);
                String value = line.substring(index + 1);
                arguments.put(key, value);

                if (key.endsWith("[]")) {
                    bufferKeys.add(key);
                }
            }
        }

        for (String bufferKey : bufferKeys) {
            int length = Integer.parseInt(arguments.get(bufferKey));
            String[] data = new String[length];
            for (int i = 0; i < length; i++) {
                data[i] = reader.readLine();
            }
            buffers.put(bufferKey, data);
        }
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getSingleValuedArgument(String name) {
        if (!name.endsWith("[]")) {
            return arguments.get(name);
        }
        return null;
    }

    public String[] getArrayArgument(String name) {
        return buffers.get(name + "[]");
    }
}
