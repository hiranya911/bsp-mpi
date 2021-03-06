package edu.ucsb.cs.bsp;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class MPIFunctionCallHandler implements Runnable {

    private ServerSocket serverSocket;
    private Socket socket;
    private AtomicBoolean status;
    private BSPPeer<NullWritable,NullWritable,Text,
            NullWritable,BytesWritable> peer;

    public MPIFunctionCallHandler(ServerSocket serverSocket, Socket socket,
                                  AtomicBoolean status,
                                  BSPPeer<NullWritable,NullWritable,Text,NullWritable,BytesWritable> peer) {
        this.serverSocket = serverSocket;
        this.socket = socket;
        this.status = status;
        this.peer = peer;
    }

    public void run() {
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            byte[] data = new byte[8096];
            int length;
            while (true) {
                boolean dirty = false;
                MPIFunctionCall function = new MPIFunctionCall();
                while((length = in.read(data)) != -1) {
                    dirty = true;
                    function.consume(data, 0, length);
                    if (function.isComplete()) {
                        dirty = false;
                        if (!function.execute(peer, out)) {
                            closeSilently();
                            return;
                        }
                        break;
                    }
                }

                if (!function.isComplete()) {
                    if (dirty) {
                        // We were cut off while in the middle of reading
                        // something. Not good.
                        throw new IOException("Socket closed prematurely: " +
                                function.toString());
                    } else {
                        // Remote process exited gracefully - Not bad
                        socket.close();
                        return;
                    }
                }
            }
        } catch (Exception e) {
            try {
                MPI2BSPTask.write(peer, "A fatal exception occurred: " + e.getMessage());
                for (StackTraceElement element : e.getStackTrace()) {
                    MPI2BSPTask.write(peer, element.toString());
                }
            } catch (IOException ignored) {
            }
            closeSilently();
        }
    }

    private void closeSilently() {
        if (status.compareAndSet(true, false)) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {

            }
        }

        try {
            socket.close();
        } catch (IOException ignored) {

        }
    }
}
