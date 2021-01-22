package programs.rxjava2;

import io.reactivex.CompletableObserver;
import io.reactivex.MaybeObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.annotations.Nullable;
import io.reactivex.disposables.Disposable;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.observers.DisposableSingleObserver;
import programs.IeRuntimeException;
import programs.delegates.IeApiResult;
import programs.delegates.IeTaskDelegate;
import programs.delegates.RxDisposeDelegate;

import java.util.logging.Level;

public abstract class AbstractRxTask<T> implements IeTaskDelegate, RxDisposeDelegate {

    private Disposable disposable;

    public IeApiResult<T> callback;

    protected boolean isForHttp() { return false; }

    protected final String getLogTag() { return this.getClass().getSimpleName(); }

    protected final void setDisposable(@NonNull Disposable disposable) { this.disposable = disposable; }

    @Override
    public final void rxDisposeIfPossible() {
        if (null != disposable) {
            if (!disposable.isDisposed()) {
                disposable.dispose();
                java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.WARNING, "rxDisposableIfNeeded - dispose");
            }
            disposable = null;
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.WARNING, "rxDisposableIfNeeded - reset");
        }
    }

    public final class ApiDisposableObserver extends DisposableObserver<T> {

        private T cachedData;

        @Override
        public void onNext(@NonNull T result) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiDisposableObserver - onNext - Thread: " + Thread.currentThread().getName());
            cachedData = result;
        }

        @Override
        public void onError(@NonNull Throwable cause) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.SEVERE, "ApiDisposableObserver - onError - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnError(cause);
        }

        @Override
        public void onComplete() {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiDisposableObserver - onComplete - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnSuccess(cachedData);
        }
    }

    public final class ApiDisposableSingleObserver extends DisposableSingleObserver<T> {

        @Override
        public void onSuccess(@NonNull T result) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiDisposableSingleObserver - onSuccess - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnSuccess(result);
        }

        @Override
        public void onError(@NonNull Throwable cause) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.SEVERE, "ApiDisposableSingleObserver - onSuccess - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnError(cause);
        }
    }

    public final class ApiMaybeObserver implements MaybeObserver<T> {

        @Override
        public void onSubscribe(@NonNull Disposable disposable) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiMaybeObserver - onSubscribe - Thread: " + Thread.currentThread().getName());
            setDisposable(disposable);
        }

        @Override
        public void onSuccess(@NonNull T result) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiMaybeObserver - onSuccess - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnSuccess(result);
        }

        @Override
        public void onError(@NonNull Throwable cause) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.SEVERE, "ApiMaybeObserver - onError - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnError(cause);
        }

        @Override
        public void onComplete() {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiMaybeObserver - onComplete - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            if (null != callback) {
                callback.onError(new IeRuntimeException("filter returns negative result", "99953"));
            }
        }
    }

    public final class ApiCompletableObserver implements CompletableObserver {

        final T result;

        public ApiCompletableObserver(@NonNull T result) { this.result = result; }

        @Override
        public void onSubscribe(@NonNull Disposable disposable) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiCompletableObserver - onSubscribe - Thread: " + Thread.currentThread().getName());
            setDisposable(disposable);
        }

        @Override
        public void onComplete() {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.INFO, "ApiCompletableObserver - onComplete - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnSuccess(result);
        }

        @Override
        public void onError(@NonNull Throwable cause) {
            java.util.logging.Logger.getLogger("AbstractRxTask").log(Level.SEVERE, "ApiCompletableObserver - onError - Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            notifyCallbackOnError(cause);
        }
    }

    protected final void notifyCallbackOnSuccess(@NonNull T result) {
        if (null != callback) {
            callback.onSuccess(result);
        }
    }

    protected final void notifyCallbackOnError(@NonNull Throwable cause) {
        if (null != callback) {
            final IeRuntimeException exception = (cause instanceof IeRuntimeException)
                    ? (IeRuntimeException)cause : new IeRuntimeException(cause, "00000");
            callback.onError(exception);
        }
    }

    ///

    public static <T extends AbstractRxTask<?>> void releaseTaskIfPossible(@Nullable T task) {
        if (null != task) {
            task.rxDisposeIfPossible();
        }
    }
}

