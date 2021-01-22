package programs.rxjava2;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import programs.IeApiResponse;
import programs.models.IePair;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.logging.Level;


public final class AppRxTask {

    public static abstract class WithExecutor<T> extends AbstractRxTask<T> implements Callable<T> {

        private final Executor executor;

        public WithExecutor(@NonNull Executor executor) { this.executor = executor; }

        @Override
        public final void runTask() {
            rxDisposeIfPossible();
            java.util.logging.Logger.getLogger("AppRxTask").log(Level.INFO, "WithExecutor - runTask - on Thread: " + Thread.currentThread().getName());
            setDisposable(
                    Single.fromCallable(this)
                            .subscribeOn(Schedulers.from(executor))
                            //.observeOn(AndroidSchedulers.mainThread())
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }

    public static abstract class WithRxIo<T> extends AbstractRxTask<T> implements Callable<T> {
        @Override
        public final void runTask() {
            rxDisposeIfPossible();
            java.util.logging.Logger.getLogger("AppRxTask").log(Level.INFO, "WithRxIo - runTask - on Thread: " + Thread.currentThread().getName());
            setDisposable(
                    Single.fromCallable(this)
                            .subscribeOn(Schedulers.io())
                            //.observeOn(AndroidSchedulers.mainThread())
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }

    public static abstract class WithRxComputation<T> extends AbstractRxTask<T> implements Callable<T> {
        @Override
        public final void runTask() {
            rxDisposeIfPossible();
            java.util.logging.Logger.getLogger("AppRxTask").log(Level.INFO, "WithRxComputation - runTask - on Thread: " + Thread.currentThread().getName());
            setDisposable(
                    Single.fromCallable(this)
                            .subscribeOn(Schedulers.computation())
                            //.observeOn(AndroidSchedulers.mainThread())
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }


    // [start] added in 2020/12/23

    public static abstract class AbsParallelCommunicationTask<T, R>
            extends AbstractRxTask<List<IePair<T, IeApiResponse<R>>>> {

        private final List<T> productList;

        public AbsParallelCommunicationTask(@NonNull final List<T> productList) {
            this.productList = productList;
        }

        @Override
        public final void runTask() {
            java.util.logging.Logger.getLogger("AppRxTask").log(Level.INFO, "AbsParallelCommunicationTask - runTask - on Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            setDisposable(
                    Flowable.just(productList)
                            .flatMap(getParallelFlatMapper())
                            .toList() // convert into Single
                            .subscribeOn(Schedulers.io())
                            //.observeOn(AndroidSchedulers.mainThread())
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }

        @NonNull
        protected abstract Function<List<T>, Publisher<IePair<T, IeApiResponse<R>>>> getParallelFlatMapper();
    }

    public static abstract class AbsParallelCommunicationFlatMapper<T, R>
            implements Function<List<T>, Publisher<IePair<T, IeApiResponse<R>>>> {

        private final int threadNumberUpperBound;

        public AbsParallelCommunicationFlatMapper() { this.threadNumberUpperBound = 24; }

        public AbsParallelCommunicationFlatMapper(final int threadNumber) { this.threadNumberUpperBound = threadNumber; }

        @Override
        public final Publisher<IePair<T, IeApiResponse<R>>> apply(@NonNull final List<T> productList) throws Exception {
            java.util.logging.Logger.getLogger("AppRxTask").log(Level.INFO, "AbsParallelCommunicationFlatMapper - runTask - on Thread: " + Thread.currentThread().getName());
            final int evaluatedThreadCt = Runtime.getRuntime().availableProcessors() * 3;
            final int threadCt = Math.min(24, evaluatedThreadCt);
            return Flowable.fromIterable(productList)
                    .parallel(threadCt)
                    .runOn(Schedulers.io())
                    .map(getSingleCommunicationFlatMapper())
                    .sequential();
        }

        @NonNull
        protected abstract Function<T, IePair<T, IeApiResponse<R>>> getSingleCommunicationFlatMapper();
    }

    // [end] added in 2020/12/23
}
