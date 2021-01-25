package programs;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import org.jetbrains.annotations.NotNull;


public final class LongRunningTask implements Runnable {

    private final byte[] lock = new byte[0];
    private boolean isStillLongRunning = false;
    private ExecutorService executorService;
    private Runnable task;

    private String getLogTag() { return this.getClass().getSimpleName(); }

    public void setTask(@NotNull final Runnable task) { this.task = task; }

    public void startTask() {
        if (null != executorService) {
            java.util.logging.Logger.getLogger("LongRunningTask").log(Level.SEVERE, "startTask - executorService existed !!");
            return;
        }

        executorService = Executors.newSingleThreadExecutor();
        java.util.logging.Logger.getLogger("LongRunningTask").log(Level.INFO, "startTask - Executors.newSingleThreadExecutor() !!");
        modifyIsStillLongRunningFlag(true);
        executorService.submit(this);
        java.util.logging.Logger.getLogger("LongRunningTask").log(Level.INFO, "startTask - executorService.submit!!");
    }

    public void stopTask() {
        modifyIsStillLongRunningFlag(false);

        if (null != executorService) {
            if (! executorService.isShutdown()) {
                try {
                    executorService.shutdownNow();
                    executorService = null;
                    java.util.logging.Logger.getLogger("LongRunningTask").log(Level.INFO, "stopTask - executorService.shutdownNow!!");
                }
                catch (Exception cause) {
                    java.util.logging.Logger.getLogger("LongRunningTask").log(Level.SEVERE, "stopTask - error on ExecutorService.shutdown(): " + cause.getLocalizedMessage());
                }
            }
            else {
                executorService = null;
                java.util.logging.Logger.getLogger("LongRunningTask").log(Level.INFO, "stopTask - executorService has been shutdown!!");
            }
        }
        else {
            java.util.logging.Logger.getLogger("LongRunningTask").log(Level.INFO, "stopTask - executorService is null!!");
        }
    }

    public boolean isStillLongRunning() { return this.isStillLongRunning; }

    public void modifyIsStillLongRunningFlag(final boolean flag) {
        synchronized(lock) {
            isStillLongRunning = flag;
        }
        java.util.logging.Logger.getLogger("LongRunningTask").log(Level.SEVERE, "modifyIsStillLongRunningFlag - result: " + isStillLongRunning);
    }

    @Override
    public void run() {
        while (isStillLongRunning) {
            if (null != this.task) { this.task.run(); }
            else {
                java.util.logging.Logger.getLogger("LongRunningTask").log(Level.SEVERE, "run - this.task is null!!");
            }
        }
    }
}
