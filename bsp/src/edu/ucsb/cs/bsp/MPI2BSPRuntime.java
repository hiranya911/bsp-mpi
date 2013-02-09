package edu.ucsb.cs.bsp;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;

import java.io.BufferedWriter;
import java.io.IOException;

public class MPI2BSPRuntime {

    private BSPPeer<NullWritable,NullWritable,Text,NullWritable,BytesWritable> peer;
    private Process mpiProcess;
    private BufferedWriter writer;

    public MPI2BSPRuntime(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, BytesWritable> peer, Process mpiProcess, BufferedWriter writer) {
        this.peer = peer;
        this.mpiProcess = mpiProcess;
        this.writer = writer;
    }

    public boolean execute(MPIFunctionCall function) throws IOException, InterruptedException {
        String functionName = function.getFunctionName();
        MPI2BSPTask.write(peer, functionName);
        if (functionName == null) {
            mpiProcess.waitFor();
            int status = mpiProcess.exitValue();
            MPI2BSPTask.write(peer, "MPI process exited with status " + status);
            return false;
        } else if ("MPI_Init".equals(functionName)) {
            // Ignore
        } else if ("MPI_Comm_rank".equals(functionName)) {
            writer.write(peer.getPeerIndex() + "\n");
            //writer.write("0\n");
            writer.flush();
        } else if ("MPI_Comm_size".equals(functionName)) {
            writer.write(peer.getNumPeers() + "\n");
            //writer.write("1\n");
            writer.flush();
        } else if ("MPI_Finalize".equals(functionName)) {
            return false;
        } else {
            throw new MPI2BSPException("Unrecognized function call: " + functionName);
        }
        return true;
    }
}
