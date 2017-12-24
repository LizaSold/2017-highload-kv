package ru.mail.polis.lizasold;

import com.sun.net.httpserver.HttpExchange;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class ServiceManager {
    private int[] ports;
    private String[] hosts;
    private int myPort;
    @NotNull
    private final MyDAO dao;
    @NotNull
    private Set<String> topology;
    private int code;
    private int goodReplicas;
    private int emptyReplicas;
    private int deletedReplicas;
    TopologyParams tp;

    public ServiceManager(int port, @NotNull MyDAO dao, @NotNull Set<String> topology) {
        this.topology = new HashSet<>(topology);
        tp = new TopologyParams(topology);
        this.hosts = tp.getHosts(topology);
        this.ports = tp.getPorts(topology);
        this.myPort = port;
        this.dao = dao;
        this.code = 0;
    }

    public void requestGet(@NotNull HttpExchange http, String id, int ack, int from) throws IOException {
        goodReplicas = 0;
        emptyReplicas = 0;
        deletedReplicas = 0;

        byte[] getValue = {};

        for (int i = 0; goodReplicas + emptyReplicas < from && i < hosts.length; i++) {
            if (ports[i] == myPort) {
                if (checkMyport(i, id)) {
                    try {
                        if (dao.isDeleted(id)) deletedReplicas++;
                        else if (dao.get(id).length == 0) emptyReplicas++;
                        else {
                            getValue = dao.get(id);
                            goodReplicas++;
                        }
                    } catch (NoSuchElementException e) {
                        continue;
                    }
                } else emptyReplicas++;
                continue;

            }
            try {
                HttpResponse res;
                res = Request.Get(getUrl(ports[i], id)).execute().returnResponse();
                code = res.getStatusLine().getStatusCode();
            } catch (IOException e) {
                continue;
            }
            getValue = getValueNew(i, id);
            if (code == 404) {
                if (MyFileDAO.del = true) deletedReplicas++;
                else emptyReplicas++;
            }
            countReplicas(code);
        }

        if ((goodReplicas + emptyReplicas + deletedReplicas) < ack) {
            throw new NullPointerException("Not enough replicas");
        } else if (deletedReplicas > 0 || emptyReplicas >= ack) {
            throw new NoSuchElementException("Not found");
        } else {
            http.sendResponseHeaders(200, getValue.length);
            http.getResponseBody().write(getValue);
        }


        http.close();
    }

    public void requestPut(@NotNull HttpExchange http, String id, int ack, int from) throws IOException {
        goodReplicas = 0;

        byte[] putValue = {};
        putValue = putValueNew(http.getRequestBody());
        for (int i = 0; goodReplicas < from && i < hosts.length; i++) {
            if (ports[i] == myPort) {
                if (checkMyport(i, id)) {
                    dao.upsert(id, putValue);
                    break;
                }
                dao.upsert(id, putValue);
                goodReplicas++;
                continue;
            }

            try {
                HttpResponse res = Request.Put(getUrl(ports[i], id)).bodyByteArray(putValue).execute().returnResponse();
                code = res.getStatusLine().getStatusCode();
            } catch (IOException e) {
                continue;
            }
            countReplicas(code);
            if (code == 404) emptyReplicas++;
        }

        if (goodReplicas < ack) throw new NullPointerException("Not enough replicas");
        else http.sendResponseHeaders(201, 0);

        http.close();
    }


    public void requestDelete(@NotNull HttpExchange http, String id, int ack, int from) throws IOException {
        goodReplicas = 0;
        emptyReplicas = 0;
        deletedReplicas = 0;
        for (int i = 0; goodReplicas < from && i < hosts.length; i++) {
            if (ports[i] == myPort) {
                if (checkMyport(i, id)) {
                    dao.delete(id);
                    goodReplicas++;
                }
                continue;
            }

            HttpResponse res;
            try {
                res = Request.Delete(getUrl(ports[i], id)).execute().returnResponse();
            } catch (IOException e) {
                continue;
            }
            code = res.getStatusLine().getStatusCode();
            countReplicas(code);

        }

        if (goodReplicas < ack) throw new NullPointerException("Not enough replicas");
        else http.sendResponseHeaders(202, 0);

        http.close();

    }

    @NotNull
    private String getUrl(final int port, @NotNull final String id) {
        return "http://localhost:" + port + "/v0/entity?id=" + id;
    }

    void countReplicas(int code) {
        if (code == 200 || code == 201 || code == 202) goodReplicas++;
    }

    private boolean checkMyport(int i, @NotNull final String id) throws IOException {
        if (dao.isExist(id) || dao.isDeleted(id)) return true;
        return false;
    }

    public byte[] getValueNew(int i, String id) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            HttpResponse res;
            res = Request.Get(getUrl(ports[i], id)).execute().returnResponse();
            res.getEntity().writeTo(out);
            return out.toByteArray();
        }
    }

    public byte[] putValueNew(InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte putValue[] = new byte[1024];
            int contentLenght = in.read(putValue);
            if (contentLenght > 0) out.write(putValue, 0, contentLenght);
            return out.toByteArray();
        }
    }
}
