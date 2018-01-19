package ru.mail.polis.lizasold;

import com.sun.net.httpserver.HttpServer;
import com.sun.org.apache.regexp.internal.RE;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import javax.management.Query;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class MyService implements KVService {
    private static final String PREFIX = "id=";
    private static final String REPLICAS = "&replicas=";
    private static final String INSIDE = "&inside";
    @NotNull
    private HttpServer server;
    @NotNull
    private MyDAO dao;
    @NotNull
    private Set<String> topology;
    ServiceManager sm;
    PutValueNew pv;


    public MyService(int port, @NotNull MyDAO dao, @NotNull Set<String> topology) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.topology = new HashSet<>();
        sm = new ServiceManager(port, dao, topology);
        pv = new PutValueNew();
        createContext();
    }

    private void createContext() {
        this.server.createContext("/v0/status", http -> {
            final String response = "ONLINE";
            http.sendResponseHeaders(200, response.length());
            http.getResponseBody().write(response.getBytes());
            http.close();
        });

        this.server.createContext("/v0/entity", new ErrorHandler(http -> {
            String query = http.getRequestURI().getQuery();
            boolean inside = false;
            if (query.contains(INSIDE)) {
                inside = true;
                query = query.substring(0, query.indexOf(INSIDE));
            }
            final String id = extractId(query);
            if (!query.contains(REPLICAS)) {
                switch (http.getRequestMethod()) {
                    case "GET":
                        if (inside && dao.isDeleted(id)) {
                            http.sendResponseHeaders(202, 0);
                        } else {
                            final byte[] getValue = dao.get(id);
                            http.sendResponseHeaders(200, getValue.length);
                            http.getResponseBody().write(getValue);
                        }
                        break;
                    case "PUT":
                        byte[] putValue = pv.putValueNew(http.getRequestBody());
                        dao.upsert(id, putValue);
                        http.sendResponseHeaders(201, 0);
                        break;
                    case "DELETE":
                        dao.delete(id);
                        http.sendResponseHeaders(202, 0);
                        break;
                    default:
                        http.sendResponseHeaders(405, 0);
                        break;
                }
            } else {
                final String replicas = extractReplicas(query);
                int ack = topology.size() / 2 + 1;
                int from = topology.size();
                ack = Integer.valueOf(replicas.split("/")[0]);
                from = Integer.valueOf(replicas.split("/")[1]);
                if (ack > from || ack == 0 || from == 0) {
                    throw new IllegalArgumentException("Check replicas");
                }
                switch (http.getRequestMethod()) {
                    case "GET":
                        sm.requestGet(http, id, ack, from);
                        break;
                    case "PUT":
                        sm.requestPut(http, id, ack, from);
                        break;
                    case "DELETE":
                        sm.requestDelete(http, id, ack, from);
                        break;

                    default:
                        http.sendResponseHeaders(405, 0);
                        break;
                }
            }
            http.close();
        }));
    }

    @NotNull
    private static String extractId(@NotNull final String query) {
        if (!query.startsWith(PREFIX)) {
            throw new IllegalArgumentException("WHAT?");
        }
        String id = query.substring(PREFIX.length());
        if (query.contains(REPLICAS)) {
            id = id.substring(0, id.indexOf(REPLICAS));
        }
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Check id");
        }
        return id;
    }

    @NotNull
    private static String extractReplicas(@NotNull final String query) {
        if (!query.matches("\\S*" + REPLICAS + "\\d*/\\d*")) {
            throw new IllegalArgumentException("Check replicas");
        }
        String replicas = query.substring(query.lastIndexOf(REPLICAS) + REPLICAS.length());
        return replicas;
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }

}
