package programs.delegates;

import org.jetbrains.annotations.NotNull;
import programs.IeRuntimeException;


public interface IeApiResult<T> {
    void onSuccess(@NotNull final T data);
    void onError(@NotNull final IeRuntimeException cause);
}
