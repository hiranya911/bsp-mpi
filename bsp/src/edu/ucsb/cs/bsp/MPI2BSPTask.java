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
import java.net.URI;
import java.util.UUID;

public class MPI2BSPTask extends BSP<NullWritable,NullWritable,Text,
        NullWritable,BytesWritable> {

    private File taskDirectory;
    private String mpiExecutable;
    private File inputMetaFile;
    private File outputMetaFile;

    public static void main(String[] args) throws Exception {
        MPI2BSPTask task = new MPI2BSPTask();
        task.setup(null);
        task.bsp(null);
        task.cleanup(null);
    }

    @Override
    public void bsp(BSPPeer<NullWritable, NullWritable, Text,
            NullWritable, BytesWritable> peer) throws IOException,
            SyncException, InterruptedException {

        String[] env = new String[] {
            "bsp.mpi.imf=" + inputMetaFile.getAbsolutePath(),
            "bsp.mpi.omf=" + outputMetaFile.getAbsolutePath()
        };
        Process process = Runtime.getRuntime().exec(mpiExecutable, env);
        write(peer, "Started the MPI process");

        BufferedReader reader = new BufferedReader(new FileReader(inputMetaFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputMetaFile));
        MPI2BSPRuntime runtime = new MPI2BSPRuntime(peer, process, writer);
        while (true) {
            MPIFunctionCall function = new MPIFunctionCall(reader);
            if (!runtime.execute(function)) {
                break;
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

        inputMetaFile = new File(taskDirectory, "input.meta");
        Runtime.getRuntime().exec("mkfifo " +
                inputMetaFile.getAbsolutePath()).waitFor();
        outputMetaFile = new File(taskDirectory, "output.meta");
        Runtime.getRuntime().exec("mkfifo " +
                outputMetaFile.getAbsolutePath()).waitFor();
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
}
