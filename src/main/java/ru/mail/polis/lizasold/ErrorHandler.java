package ru.mail.polis.lizasold;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ErrorHandler implements HttpHandler {
    private final HttpHandler delegate;

    public ErrorHandler(HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        try {
            delegate.handle(http);
        } catch (NoSuchElementException e) {
            http.sendResponseHeaders(404, 0);
            http.close();
        } catch (IllegalArgumentException e) {
            http.sendResponseHeaders(400, 0);
            http.close();
        } catch (NullPointerException e) {
            http.sendResponseHeaders(504, 0);
            http.close();
        }
    }
}


