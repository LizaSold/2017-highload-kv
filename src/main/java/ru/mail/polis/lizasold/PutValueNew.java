package ru.mail.polis.lizasold;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class PutValueNew {
    public byte[] putValueNew(InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] putValue = new byte[1024];
            int contentLenght = 0;
            while (!(contentLenght == -1)) {
                contentLenght = in.read(putValue);
                if (contentLenght > 0) out.write(putValue, 0, contentLenght);
            }
            return out.toByteArray();
        }
    }
}
