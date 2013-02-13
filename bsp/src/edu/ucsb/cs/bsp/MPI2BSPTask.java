package edu.ucsb.cs.bsp;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MPI2BSPTask extends BSP<NullWritable,NullWritable,Text,
        NullWritable,BytesWritable> {

    private File taskDirectory;
    private String mpiExecutable;

    public static void main(String[] args) throws Exception {
        MPI2BSPTask task = new MPI2BSPTask();
        task.setup(null);
        task.bsp(null);
        task.cleanup(null);
    }

    @Override
    public void bsp(final BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, BytesWritable> peer) throws IOException,
            SyncException, InterruptedException {

        final ServerSocket serverSocket = new ServerSocket(0);
        String[] env = new String[] {
            "bsp.mpi.port=" + serverSocket.getLocalPort()
        };
        Process process = Runtime.getRuntime().exec(mpiExecutable, env);
        write(peer, "Started the MPI process");

        ExecutorService exec = Executors.newCachedThreadPool();
        AtomicBoolean status = new AtomicBoolean(true);
        MPIDeadProcessCallback callback = new MPIDeadProcessCallback(serverSocket, status);
        ProcessHealthChecker healthChecker = new ProcessHealthChecker(process, callback);
        healthChecker.start();
        while (status.get()) {
            try {
                Socket socket = serverSocket.accept();
                MPIFunctionCallHandler handler = new MPIFunctionCallHandler(
                        serverSocket, socket, status, peer);
                exec.submit(handler);
            } catch (IOException e) {
                if (status.get()) {
                    throw e;
                } else {
                    break;
                }
            }
        }
        exec.shutdown();
        healthChecker.stop();

        BufferedReader out = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        String str;
        while ((str = out.readLine()) != null) {
            write(peer, str);
        }
        out.close();

        process.waitFor();
        int exitStatus = process.exitValue();
        write(peer, "MPI process exited with status " + exitStatus);
    }

    @Override
    public void setup(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, BytesWritable> peer) throws IOException,
            SyncException, InterruptedException {
        String taskId = UUID.randomUUID().toString().replaceAll("-", "");
        taskDirectory = new File("/tmp/mpi2bsp/tasks", taskId);
        FileUtils.forceMkdir(taskDirectory);

        Configuration configuration = peer.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        String mpiBinaryPath = configuration.get(MPI2BSPJob.MPI_BINARY_PATH);
        Path src = new Path(URI.create(mpiBinaryPath));
        File executable = new File(taskDirectory, src.getName());
        mpiExecutable = executable.getAbsolutePath();
        Path dest = new Path(mpiExecutable);
        fs.copyToLocalFile(false, src, dest);
        //mpiExecutable = "/Users/hiranya/Projects/bsp-mpi/impl/bsp-mpi/mpi/a.out";
    }

    @Override
    public void cleanup(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, BytesWritable> peer) throws IOException {
        FileUtils.deleteDirectory(taskDirectory);
    }

    public static void write(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, BytesWritable> peer, String msg) throws IOException {
        peer.write(new Text(msg), NullWritable.get());
        //System.out.println(msg);
    }

    private class MPIDeadProcessCallback implements DeadProcessCallback {

        private ServerSocket serverSocket;
        private AtomicBoolean status;

        private MPIDeadProcessCallback(ServerSocket serverSocket,
                                       AtomicBoolean status) {
            this.serverSocket = serverSocket;
            this.status = status;
        }

        @Override
        public void notifyDeadProcess(int s) {
            if (status.compareAndSet(true, false)) {
                try {
                    serverSocket.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
}
