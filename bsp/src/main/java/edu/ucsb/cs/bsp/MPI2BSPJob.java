package edu.ucsb.cs.bsp;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class MPI2BSPJob {

    public static final String MPI_BINARY_PATH = "mpi.binary.path";

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            throw new MPI2BSPException("Required command line arguments not provided");
        }

        String mpiBinaryPath = args[0];
        int numProcesses = Integer.parseInt(args[1]);

        File mpiBinaryFile = new File(mpiBinaryPath);
        if (!mpiBinaryFile.exists()) {
            throw new MPI2BSPException("Specified MPI binary file does not exist");
        } else if (!mpiBinaryFile.isFile()) {
            throw new MPI2BSPException("Specified MPI binary is not a regular file");
        } else if (!mpiBinaryFile.canExecute()) {
            throw new MPI2BSPException("Specified MPI binary is not executable");
        }

        HamaConfiguration configuration = new HamaConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        String jobId = UUID.randomUUID().toString().replaceAll("-", "");
        Path parent = new Path("/tmp/mpi2bsp/jobs", jobId);
        fs.mkdirs(parent);
        Path src = new Path(mpiBinaryPath);
        Path dest = new Path(parent, mpiBinaryFile.getName());
        // Upload the executable from local file system to HDFS
        fs.copyFromLocalFile(false, src, dest);

        configuration.set(MPI_BINARY_PATH, dest.toUri().toString());

        BSPJob job = new BSPJob(configuration);
        job.setBspClass(MPI2BSPTask.class);
        job.setJarByClass(MPI2BSPTask.class);
        job.setJobName("MPI-to-BSP job " + jobId);
        job.setNumBspTask(numProcesses);
        Path outputDir = new Path(parent, "output");
        job.setOutputPath(outputDir);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.waitForCompletion(true);

        // Download output from HDFS to local file system
        Path localOutputDir = new Path("/tmp/output");
        fs.copyToLocalFile(false, outputDir, localOutputDir);
        fs.delete(parent, true);
    }

}
