package de.SweetCode.SweetDB;

/**
 * Created by Yonas on 15.04.2016.
 */
public class Optional<T> {

    private static final Optional<?> EMPTY = new Optional();

    private final T value;

    private Optional() {
        this.value = null;
    }

    private Optional(T value) {
        this.value = value;
    }

    public static <T> Optional<T> empty() {
        return (Optional<T>) Optional.EMPTY;
    }

    public static <T> Optional<T> of(T value) {
        return new Optional<>(value);
    }


    public T get() {
        if (!(this.isPresent())) {
            throw new NullPointerException("Value is not present.");
        }

        return this.value;
    }

    public boolean isPresent() {
        return (!(this.value == null));
    }

    public T orElse(T other) {
        return (this.value == null ? other : this.value);
    }

    @Override
    public String toString() {
        return value == null ? "Optional.empty" : String.format("Optional[%s]", this.value);
    }

}
