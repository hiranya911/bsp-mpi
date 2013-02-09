package edu.ucsb.cs.bsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import java.io.*;
import java.util.UUID;

public class MPI2BSPTask extends BSP<NullWritable,NullWritable,Text,
        NullWritable,NullWritable> {

    private static final Object lock = new Object();

    public static void main(String[] args) throws Exception {
        MPI2BSPTask task = new MPI2BSPTask();
        task.bsp(null);
    }

    @Override
    public void bsp(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, NullWritable> peer) throws IOException,
            SyncException, InterruptedException {

        String jobId = UUID.randomUUID().toString().replaceAll("-", "");
        File jobDir = new File("/tmp/jobs", jobId);
        jobDir.mkdirs();
        File inputMetaFile = new File(jobDir, "input.meta");
        File outputMetaFile = new File(jobDir, "output.meta");
        Process p = Runtime.getRuntime().exec("mkfifo " + inputMetaFile.getAbsolutePath());
        p.waitFor();
        p = Runtime.getRuntime().exec("mkfifo " + outputMetaFile.getAbsolutePath());
        p.waitFor();
        write(peer, "Created named pipes");

        Configuration configuration = peer.getConfiguration();
        String cmd = configuration.get(MPI2BSPJob.MPI_BINARY_PATH);
        //String cmd = "/Users/hiranya/Projects/bsp-mpi/impl/bsp-mpi/mpi/a.out";

        String[] env = new String[] {
                "bsp.mpi.imf=" + inputMetaFile.getAbsolutePath(),
                "bsp.mpi.omf=" + outputMetaFile.getAbsolutePath()
        };
        Process process = Runtime.getRuntime().exec(cmd, env);
        write(peer, "Started MPI process");

        boolean finished = false;
        BufferedReader reader = new BufferedReader(new FileReader(inputMetaFile));
        System.out.println("Opened reader");
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputMetaFile));
        System.out.println("Opened writer");
        while (!finished) {
            MPIFunctionData function = new MPIFunctionData(reader);
            String functionName = function.getFunctionName();
            write(peer, functionName);
            if ("MPI_Init".equals(functionName)) {

            } else if ("MPI_Comm_rank".equals(functionName)) {
                writer.write(peer.getPeerIndex() + "\n");
                //writer.write("0\n");
                writer.flush();
            } else if ("MPI_Comm_size".equals(functionName)) {
                writer.write(peer.getNumPeers() + "\n");
                //writer.write("1\n");
                writer.flush();
            } else if ("MPI_Finalize".equals(functionName)) {
                finished = true;
            } else {
                throw new RuntimeException("Unrecognized function call: " + functionName);
            }
        }
        reader.close();
        writer.close();

        BufferedReader out = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        String str;
        while ((str = out.readLine()) != null) {
            write(peer, str);
        }
        out.close();
        process.waitFor();

        inputMetaFile.delete();
        outputMetaFile.delete();
        jobDir.delete();
    }

    private void write(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, NullWritable> peer, String msg) throws IOException {
        peer.write(new Text(msg), NullWritable.get());
        //System.out.println(msg);
    }
}
