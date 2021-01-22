package programs.models;

import org.jetbrains.annotations.NotNull;

public final class IePair<T, R> {

    public final T first;
    public final R second;

    public IePair(@NotNull final T first, @NotNull final R second) {
        this.first = first;
        this.second = second;
    }

    @NotNull
    @Override
    public String toString() { return "IePair {first: " + first + ", second: " + second + "}"; }
}
