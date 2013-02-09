package edu.ucsb.cs.bsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.UUID;

public class MPI2BSPTask extends BSP<NullWritable,NullWritable,Text,
        NullWritable,NullWritable> {

    private static final Object lock = new Object();

    @Override
    public void bsp(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, NullWritable> peer) throws IOException,
            SyncException, InterruptedException {

        String jobId = UUID.randomUUID().toString().replaceAll("-", "");
        File jobDir = new File("/tmp/jobs", jobId);
        jobDir.mkdirs();

        Configuration configuration = peer.getConfiguration();
        String cmd = configuration.get(MPI2BSPJob.MPI_BINARY_PATH);

        Signal signal = new Signal("USR1");
        Signal.handle(signal, new MPI2BSPSignalHandler());
        int pid = getProcessId();

        File inputMetaFile = new File(jobDir, "input.meta");
        File outputMetaFile = new File(jobDir, "output.meta");

        String[] env = new String[] {
                "bsp.mpi.imf=" + inputMetaFile.getAbsolutePath(),
                "bsp.mpi.omf=" + outputMetaFile.getAbsolutePath(),
                "bsp.mpi.parent=" + pid
        };
        Process process = Runtime.getRuntime().exec(cmd, env);

        int childProcess = -1;
        boolean finished = false;
        while (!finished) {
            synchronized (lock) {
                if (!inputMetaFile.exists()) {
                    try {
                        int status = process.exitValue();
                        write(peer, "Process exited with status " + status);
                        break;
                    } catch (IllegalThreadStateException e) {
                        lock.wait(10000);
                        continue;
                    }
                }

                BufferedReader reader = new BufferedReader(new FileReader(inputMetaFile));
                String line = reader.readLine();
                if ("MPI_Init".equals(line)) {
                    childProcess = Integer.parseInt(reader.readLine());
                } else if ("MPI_Comm_rank".equals(line)) {
                    File tempFile = new File(jobDir, "temp");
                    BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
                    writer.write(peer.getPeerIndex() + "\n");
                    writer.close();
                    tempFile.renameTo(outputMetaFile);
                } else if ("MPI_Comm_size".equals(line)) {
                    File tempFile = new File(jobDir, "temp");
                    BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
                    writer.write(peer.getAllPeerNames().length + "\n");
                    writer.close();
                    tempFile.renameTo(outputMetaFile);
                } else if ("MPI_Finalize".equals(line)) {
                    finished = true;
                } else {
                    throw new RuntimeException("Unrecognized function call: " + line);
                }

                reader.close();
                if (!inputMetaFile.delete()) {
                    write(peer, "Failed to delete return file");
                }
                interrupt(childProcess);
            }
        }

        BufferedReader out = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        String str;
        while ((str = out.readLine()) != null) {
            write(peer, str);
        }
        out.close();
        process.waitFor();

        jobDir.delete();
    }

    private void interrupt(int processId) throws IOException {
        if (processId < 0) {
            throw new RuntimeException("Negative process ID");
        }
        String command = "kill -SIGUSR1 " + processId;
        Runtime.getRuntime().exec(command);
    }

    private int getProcessId() {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        int index = jvmName.indexOf('@');
        if (index < 1) {
            throw new RuntimeException("Failed to obtain local process ID");
        }
        return Integer.parseInt(jvmName.substring(0, index));
    }

    private static class MPI2BSPSignalHandler implements SignalHandler {
        @Override
        public void handle(Signal signal) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    private void write(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, NullWritable> peer, String msg) throws IOException {
        peer.write(new Text(msg), NullWritable.get());
    }
}
