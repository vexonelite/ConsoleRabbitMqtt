package programs.rxjava2;

import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.functions.Consumer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.SocketException;
import java.util.logging.Level;

/**
 * https://github.com/ReactiveX/RxJava/wiki/What's-different-in-2.0#error-handling
 */
public final class AppRxJavaErrorHandler implements Consumer<Throwable> {

    private final String logTag;

    public AppRxJavaErrorHandler(@NotNull String logTag) {
        this.logTag = logTag;
    }

    @Override
    public void accept(Throwable throwable) throws Exception {
        if (throwable instanceof UndeliverableException) {
            java.util.logging.Logger.getLogger("AppRxJavaErrorHandler").log(Level.SEVERE, "UndeliverableException", throwable);
            throwable = throwable.getCause();
        }
        if ((throwable instanceof IOException) || (throwable instanceof SocketException)) {
            // fine, irrelevant network problem or API that throws on cancellation
            java.util.logging.Logger.getLogger("AppRxJavaErrorHandler").log(Level.SEVERE, "IOException or SocketException", throwable);
            return;
        }
        if (throwable instanceof InterruptedException) {
            // fine, some blocking code was interrupted by a dispose call
            java.util.logging.Logger.getLogger("AppRxJavaErrorHandler").log(Level.SEVERE, "InterruptedException", throwable);
            return;
        }
        if ((throwable instanceof NullPointerException) || (throwable instanceof IllegalArgumentException)) {
            // that's likely a bug in the application
            java.util.logging.Logger.getLogger("AppRxJavaErrorHandler").log(Level.SEVERE, "NullPointerException or IllegalArgumentException [that's likely a bug in the application]: " + throwable.getLocalizedMessage());
            Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), throwable);
            return;
        }
        if (throwable instanceof IllegalStateException) {
            // that's a bug in RxJava or in a custom operator
            java.util.logging.Logger.getLogger("AppRxJavaErrorHandler").log(Level.SEVERE, "IllegalStateException [that's a bug in RxJava or in a custom operator]: " + throwable.getLocalizedMessage());
            Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), throwable);
            return;
        }
        java.util.logging.Logger.getLogger("AppRxJavaErrorHandler").log(Level.SEVERE, "Undeliverable exception received, not sure what to do: " + throwable.getLocalizedMessage());
    }
}

