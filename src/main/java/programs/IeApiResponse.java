package programs;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public final class IeApiResponse<T>  {

    @Nullable
    public final T result;

    @Nullable
    public final IeRuntimeException error;

    public IeApiResponse(@Nullable final T result, @Nullable final IeRuntimeException error) {
        this.result = result;
        this.error = error;
    }

    @NotNull
    @Override
    public String toString() { return "programs.IeApiResponse {result: " + result + ", error: " + error + "}"; }
}
