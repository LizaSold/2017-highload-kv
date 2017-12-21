package ru.mail.polis.lizasold;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.HashSet;
import java.util.NoSuchElementException;

public class MyFileDAO implements MyDAO {

    @NotNull
    private final File dir;
    @NotNull
    public HashSet<String> deletedSet;
    @NotNull
    private File getFile(@NotNull final String key){
        return new File(dir, key);
    }

    public MyFileDAO(@NotNull final File dir) {
        this.dir = dir;
        this.deletedSet = new HashSet<>();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull final String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final File file = getFile(key);
        if (!isExist(key)) {
            throw new  NoSuchElementException();
        }
        final byte[] value = new byte[(int) file.length()];
        try(InputStream is = new FileInputStream(file)) {
            is.read(value);
        }

        return value;
    }
    @Override
    public void upsert(@NotNull final String key, @NotNull final byte[] value)throws IllegalArgumentException, IOException {
        try(OutputStream os = new FileOutputStream(getFile(key))){
             os.write(value);
        }
    }

    @Override
    public void delete(@NotNull final String key) throws IllegalArgumentException, IOException {
        getFile(key).delete();
        deletedSet.add(key);
    }

    public boolean isExist(@NotNull final String key) throws IllegalArgumentException, IOException {
        File file = new File(dir, key);
        if (file.exists()) return true;
        return false;
    }

    public boolean isDeleted(@NotNull final String key) {
        if (deletedSet.contains(key)) return true;
        return false;
    }
}
