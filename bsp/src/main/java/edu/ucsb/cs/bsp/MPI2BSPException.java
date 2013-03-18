package edu.ucsb.cs.bsp;

public class MPI2BSPException extends RuntimeException {

    public MPI2BSPException(String message) {
        super(message);
    }

    public MPI2BSPException(String message, Throwable cause) {
        super(message, cause);
    }
}
