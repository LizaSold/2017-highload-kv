package ru.mail.polis.lizasold;

import com.sun.net.httpserver.HttpExchange;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ServiceManager {
    private int[] ports;
    private String[] hosts;
    private int myPort;
    @NotNull
    private final MyDAO dao;
    @NotNull
    private Set<String> topology;
    private int code;
    Map<Integer, String> tp;

    public ServiceManager(int port, @NotNull MyDAO dao, @NotNull Set<String> topology) {
        this.topology = new HashSet<>(topology);
        this.tp = TopologyParams(topology);
        this.myPort = port;
        this.dao = dao;
        this.code = 0;
    }

    public Map<Integer, String> TopologyParams(@NotNull Set<String> topology) {
        this.topology = new HashSet<>(topology);
        Map<Integer, String> tp = new HashMap<Integer, String>();
        String[] endpoints = topology.toArray(new String[topology.size()]);
        for (int i = 0; i < endpoints.length; i++) {
            tp.put(Integer.parseInt(endpoints[i].substring(endpoints[i].lastIndexOf(':') + 1)),
                    endpoints[i].substring("http://".length(), endpoints[i].lastIndexOf(':')));
        }
        return tp;
    }

    public void requestGet(@NotNull HttpExchange http, String id, int ack, int from) throws IOException {
        int goodReplicas = 0;
        int emptyReplicas = 0;
        int deletedReplicas = 0;

        byte[] getValue = {};
        for (Map.Entry<Integer, String> me : tp.entrySet()) {
            if (goodReplicas + emptyReplicas < from) {
                if (me.getKey() == myPort) {
                    if (dao.isExist(id)) {
                        try {
                            if (dao.isDeleted(id)) deletedReplicas++;
                            else if (dao.get(id).length == 0) emptyReplicas++;
                            else goodReplicas++;
                        } catch (NoSuchElementException e) {
                            continue;
                        }
                    } else emptyReplicas++;
                    continue;
                }
                try {
                    HttpResponse res;
                    res = Request.Get(getUrl(me.getKey(), id)).execute().returnResponse();
                    code = res.getStatusLine().getStatusCode();
                } catch (IOException e) {
                    continue;
                }
                getValue = getValueNew(me.getKey(), id);
                if (code == 404)
                    if (MyFileDAO.del) deletedReplicas++;
                    else emptyReplicas++;
                if (code == 200) goodReplicas++;
            }
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
        int goodReplicas = 0;
        byte[] putValue = putValueNew(http.getRequestBody());
        for (Map.Entry<Integer, String> me : tp.entrySet()) {
            if (goodReplicas < from) {
                if (me.getKey() == myPort) {
                    dao.upsert(id, putValue);
                    goodReplicas++;
                    continue;
                }
                try {
                    HttpResponse res = Request.Put(getUrl(me.getKey(), id)).bodyByteArray(putValue).execute().returnResponse();
                    code = res.getStatusLine().getStatusCode();
                } catch (IOException e) {
                    continue;
                }
                if (code == 201) goodReplicas++;
                continue;
            }
        }
        if (goodReplicas < ack) throw new NullPointerException("Not enough replicas");
        else http.sendResponseHeaders(201, 0);

        http.close();
    }


    public void requestDelete(@NotNull HttpExchange http, String id, int ack, int from) throws IOException {
        int goodReplicas = 0;
        for (Map.Entry<Integer, String> me : tp.entrySet()) {
            if (goodReplicas < from) {
                if (me.getKey() == myPort) {
                    if (dao.isExist(id)) {
                        dao.delete(id);
                        goodReplicas++;
                    }
                    continue;
                }

                HttpResponse res;
                try {
                    res = Request.Delete(getUrl(me.getKey(), id)).execute().returnResponse();
                } catch (IOException e) {
                    continue;
                }
                code = res.getStatusLine().getStatusCode();
                if (code == 202) goodReplicas++;

            }
        }
        if (goodReplicas < ack) throw new NullPointerException("Not enough replicas");
        else http.sendResponseHeaders(202, 0);

        http.close();

    }

    @NotNull
    private String getUrl(final int port, @NotNull final String id) {
        return "http://localhost:" + port + "/v0/entity?id=" + id;
    }

    public byte[] getValueNew(int portMe, String id) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            HttpResponse res;
            res = Request.Get(getUrl(portMe, id)).execute().returnResponse();
            res.getEntity().writeTo(out);
            return out.toByteArray();
        }
    }

    public byte[] putValueNew(InputStream in) throws IOException {
        long t=0;
        try(ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] putValue = new byte[1024];
            while (true) {
                int contentLenght = in.read(putValue);
                t += contentLenght;
                if (contentLenght == -1) break;
                if (contentLenght > 0) out.write(putValue, 0, contentLenght);
            }
            return out.toByteArray();

        }
    }
}
