package ru.mail.polis.lizasold;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;

public interface MyDAO {

    @NotNull
    byte[] get(@NotNull String key) throws NoSuchElementException, IOException;

    void upsert(@NotNull String key, @NotNull byte[] value) throws IOException;

    @NotNull
    void delete(@NotNull String key) throws IOException;

    void createDeletedDir();

    boolean isExist(@NotNull final String key) throws IOException;

    boolean isDeleted(@NotNull final String key) throws IOException;
}
