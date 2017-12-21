package ru.mail.polis.lizasold;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ErrorHandler implements HttpHandler{
    private final HttpHandler delegate;

    public ErrorHandler(HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        try {
            delegate.handle(httpExchange);
        } catch (NoSuchElementException e) {
            httpExchange.sendResponseHeaders(404, 0);
            httpExchange.close();
        } catch (IllegalArgumentException e) {
            httpExchange.sendResponseHeaders(400, 0);
            httpExchange.close();
                }


    }
}


