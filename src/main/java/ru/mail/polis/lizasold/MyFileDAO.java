package ru.mail.polis.lizasold;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;


public class MyFileDAO implements MyDAO {

    public static boolean del;
    @NotNull
    private final String dir;
    @NotNull
    private final String deletedDir;

    public MyFileDAO(@NotNull String dir) {
        this.dir = dir;
        this.deletedDir = dir + "/deletedDir";
        createDeletedDir();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IOException {
        del = false;
        if (isDeleted(id)) {
            del = true;
            throw new NoSuchElementException("deleted");
        } else if (!isExist(id)) {
            throw new NoSuchElementException("no file with id " + id);
        }
        return Files.readAllBytes(Paths.get(dir, id));
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IOException {
        Files.write(Paths.get(dir, id), value);
        if (isDeleted(id)) Files.delete(Paths.get(deletedDir, id));
    }

    @Override
    public void delete(@NotNull String id) throws IOException {
        Files.createFile(Paths.get(deletedDir, id));
    }

    public void createDeletedDir() {
        try {
            Files.createDirectory(Paths.get(deletedDir));
        } catch (IOException e) {
            //ignore
        }
    }

    public boolean isExist(@NotNull final String id) throws IOException {
        return Files.exists(Paths.get(dir, id));
    }

    public boolean isDeleted(@NotNull final String id) throws IOException {
        return Files.exists(Paths.get(deletedDir, id));
    }
}

