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

    @Override
    public void run() {
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            byte[] data = new byte[8096];
            int length;
            MPIFunctionCall function = new MPIFunctionCall();
            while((length = in.read(data)) != -1) {
                function.consume(data, 0, length);
                if (function.isComplete()) {
                    if (!function.execute(peer, out)) {
                        closeSilently();
                        return;
                    }
                    break;
                }
            }

            if (!function.isComplete()) {
                throw new IOException("Socket closed prematurely: " + function.toString());
            }
            socket.close();
        } catch (Exception e) {
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
