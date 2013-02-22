package edu.ucsb.cs.bsp;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProcessHealthChecker {

    private Process process;
    private DeadProcessCallback callback;
    private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

    public ProcessHealthChecker(Process process, DeadProcessCallback callback) {
        this.process = process;
        this.callback = callback;
    }

    public void start() {
        exec.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    int status = process.exitValue();
                    callback.notifyDeadProcess(status);
                } catch (IllegalThreadStateException ignored) {

                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    public void stop() {
        exec.shutdownNow();
    }

}
