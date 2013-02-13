package edu.ucsb.cs.bsp;

import java.util.HashSet;
import java.util.Set;

public class MPIMessageStore {

    private Set<MPIFunctionCall> store = new HashSet<MPIFunctionCall>();

    private static final MPIMessageStore instance = new MPIMessageStore();

    private MPIMessageStore() {

    }

    public static MPIMessageStore getInstance() {
        return instance;
    }

    public synchronized void store(MPIFunctionCall function) {
        store.add(function);
        this.notifyAll();
    }

    public MPIFunctionCall getMessage(MPIFunctionCall function) {
        MPIFunctionCall functionCall = null;
        for (MPIFunctionCall call : store) {
            if (call.getArgument("type").equals(function.getArgument("type")) &&
                    call.getArgument("tag").equals(function.getArgument("tag")) &&
                    call.getArgument("source").equals(function.getArgument("source"))) {
                functionCall = call;
            }
        }
        if (functionCall != null) {
            store.remove(functionCall);
        }
        return functionCall;
    }

}
