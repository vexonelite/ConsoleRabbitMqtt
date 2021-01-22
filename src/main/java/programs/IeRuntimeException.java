package programs;

import org.jetbrains.annotations.NotNull;


public class IeRuntimeException extends Exception {

    private final String exceptionCode;

    public IeRuntimeException(@NotNull String message, @NotNull String exceptionCode) {
        super(message);
        this.exceptionCode = exceptionCode;
    }

    public IeRuntimeException(@NotNull Throwable cause, @NotNull String exceptionCode) {
        super(cause);
        this.exceptionCode = exceptionCode;
    }

    public IeRuntimeException(@NotNull String message, @NotNull Throwable cause, @NotNull String exceptionCode) {
        super(message, cause);
        this.exceptionCode = exceptionCode;
    }

    @NotNull
    public final String getExceptionCode () { return exceptionCode; }
}
