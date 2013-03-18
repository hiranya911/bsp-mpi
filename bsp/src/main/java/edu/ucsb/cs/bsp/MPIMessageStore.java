package edu.ucsb.cs.bsp;

import java.util.ArrayList;
import java.util.List;

public class MPIMessageStore {

    private List<MPIFunctionCall> store = new ArrayList<MPIFunctionCall>();

    private static final MPIMessageStore instance = new MPIMessageStore();

    private MPIMessageStore() {

    }

    public static MPIMessageStore getInstance() {
        return instance;
    }

    public void store(MPIFunctionCall function) {
        synchronized (this) {
            store.add(function);
            this.notifyAll();
        }
    }

    public synchronized MPIFunctionCall getMessage(MPIFunctionCall function) {
        MPIFunctionCall functionCall = null;
        for (MPIFunctionCall call : store) {
            if (call.getArgument(MPIFunctionCall.MPI_TYPE).equals(
                    function.getArgument(MPIFunctionCall.MPI_TYPE)) &&
                    call.getArgument(MPIFunctionCall.MPI_TAG).equals(
                            function.getArgument(MPIFunctionCall.MPI_TAG)) &&
                    call.getArgument(MPIFunctionCall.MPI_SRC).equals(
                            function.getArgument(MPIFunctionCall.MPI_SRC)) &&
                    Integer.parseInt(call.getArgument(MPIFunctionCall.MPI_COUNT)) <=
                            Integer.parseInt(function.getArgument(MPIFunctionCall.MPI_RECV_COUNT))) {
                functionCall = call;
                break;
            }
        }
        if (functionCall != null) {
            store.remove(functionCall);
        }
        return functionCall;
    }

}
