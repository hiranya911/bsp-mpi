package edu.ucsb.cs.bsp;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class MPIFunctionCall {

    public static final String MPI_COUNT = "count";
    public static final String MPI_RECV_COUNT = "rcount";
    public static final String MPI_TAG = "tag";
    public static final String MPI_TYPE = "type";
    public static final String MPI_SRC = "source";
    public static final String MPI_DEST = "dest";

    private String functionName = null;
    private Map<String,String> arguments = new HashMap<String, String>();
    private byte[] buffer = null;

    private StringBuilder builder = new StringBuilder();
    private int bufferPosition = 0;
    private int prev = -1;
    private boolean complete = false;

    public void consume(byte[] data, int offset, int limit) {
        for (int i = offset; i < limit; i++) {
            if (complete) {
                throw new MPI2BSPException("Received more bytes than expected");
            }

            if (buffer == null) {
                if (data[i] == '\n') {
                    if (prev == '\n') {
                        if (arguments.containsKey(MPI_COUNT)) {
                            int count = Integer.parseInt(arguments.get(MPI_COUNT));
                            buffer = new byte[count];
                        } else {
                            complete = true;
                        }
                    } else if (functionName == null) {
                        functionName = builder.toString();
                    } else {
                        String argument = builder.toString();
                        int index = argument.indexOf('=');
                        String key = argument.substring(0, index);
                        String value = argument.substring(index + 1);
                        arguments.put(key, value);
                    }
                    builder = new StringBuilder();
                } else {
                    builder.append((char) data[i]);
                }
                prev = data[i];

            } else if (bufferPosition < buffer.length) {
                buffer[bufferPosition++] = data[i];
                if (bufferPosition == buffer.length) {
                    complete = true;
                }
            }
        }
    }

    public boolean execute(BSPPeer<NullWritable,NullWritable,Text,
            NullWritable,BytesWritable> peer, OutputStream out) throws IOException {
        if ("MPI_Init".equals(functionName)) {
            writeResponse("OK\0", out);
        } else if ("MPI_Comm_rank".equals(functionName)) {
            writeResponse(peer.getPeerIndex() + "\0", out);
            //writeResponse("0\0", out);
        } else if ("MPI_Comm_size".equals(functionName)) {
            writeResponse(peer.getNumPeers() + "\0", out);
            //writeResponse("1\0", out);
        } else if ("MPI_Finalize".equals(functionName)) {
            writeResponse("OK\0", out);
            return false;
        } else if ("MPI_Send".equals(functionName)) {
            String dest = peer.getPeerName(Integer.parseInt(arguments.get(MPI_DEST)));
            arguments.put(MPI_SRC, String.valueOf(peer.getPeerIndex()));
            byte[] bytes = serialize();
            peer.send(dest, new BytesWritable(bytes));
            writeResponse("OK\0", out);
            sync(peer);
        } else if ("MPI_Recv".equals(functionName)) {
            receiveMessage(peer, out);
        } else if ("MPI_Bcast".equals(functionName)) {
            int source = Integer.parseInt(arguments.get(MPI_SRC));
            arguments.put(MPI_TAG, "bcast");
            if (source == peer.getPeerIndex()) {
                String localPeer = peer.getPeerName();
                byte[] bytes = serialize();
                for (String peerName : peer.getAllPeerNames()) {
                    if (!peerName.equals(localPeer)) {
                        peer.send(peerName, new BytesWritable(bytes));
                    }
                }
                writeResponse("OK\0", out);
                sync(peer);
            } else {
                receiveMessage(peer, out);
            }
        } else {
            throw new MPI2BSPException("Unrecognized function call: " + functionName);
        }
        return true;
    }

    private void receiveMessage(BSPPeer<NullWritable,NullWritable,Text,
            NullWritable,BytesWritable> peer, OutputStream out) throws IOException {
        sync(peer);
        MPIMessageStore store = MPIMessageStore.getInstance();
        BytesWritable writable;
        while ((writable = peer.getCurrentMessage()) != null) {
            MPIFunctionCall call = new MPIFunctionCall();
            call.consume(writable.getBytes(), 0, writable.getLength());
            store.store(call);
        }

        MPIFunctionCall functionCall = store.getMessage(this);
        if (functionCall != null) {
            int length = functionCall.buffer.length;
            byte[] bytes = new byte[4];
            for (int i = 0; i < 4; i++) {
                bytes[i] = (byte)(length >>> (i * 8));
            }
            writeResponse(bytes, out);
            writeResponse(functionCall.buffer, out);
        } else {
            throw new IOException("No matching messages received");
        }
    }

    private void sync(BSPPeer<NullWritable,NullWritable,Text,
            NullWritable,BytesWritable> peer) throws IOException {
        try {
            peer.sync();
        } catch (SyncException e) {
            throw new IOException("Synchronization error", e);
        } catch (InterruptedException e) {
            throw new IOException("Barrier interrupted", e);
        }
    }

    public boolean isComplete() {
        return complete;
    }

    private void writeResponse(String data, OutputStream out) throws IOException {
        out.write(data.getBytes());
        out.flush();
    }

    private void writeResponse(byte[] data, OutputStream out) throws IOException {
        out.write(data);
        out.flush();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(functionName).append("\n");
        for (Map.Entry<String,String> entry : arguments.entrySet()) {
            builder.append(entry.getKey()).append("=").
                    append(entry.getValue()).append("\n");
        }
        builder.append("\n");
        return builder.toString();
    }

    private byte[] serialize() {
        byte[] header = toString().getBytes();
        if (buffer != null) {
            byte[] data = new byte[header.length + buffer.length];
            System.arraycopy(header, 0, data, 0, header.length);
            System.arraycopy(buffer, 0, data, header.length, buffer.length);
            return data;
        } else {
            return header;
        }
    }

    public String getArgument(String name) {
        return arguments.get(name);
    }
}
