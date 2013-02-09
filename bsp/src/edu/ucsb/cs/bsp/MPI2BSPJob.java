package edu.ucsb.cs.bsp;

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
            throw new IOException("Required command line arguments not provided");
        }

        HamaConfiguration configuration = new HamaConfiguration();
        configuration.set(MPI_BINARY_PATH, args[0]);

        BSPJob job = new BSPJob(configuration);
        job.setBspClass(MPI2BSPTask.class);
        job.setJarByClass(MPI2BSPTask.class);
        job.setJobName("MPI-to-BSP job");
        job.setNumBspTask(Integer.parseInt(args[1]));
        job.setOutputPath(new Path("/tmp/output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.waitForCompletion(true);
    }

}
