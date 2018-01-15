package ru.mail.polis.lizasold;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

public class TopologyParams {
    @NotNull
    private Set<String> topology;
    public TopologyParams(@NotNull Set<String> topology){
        this.topology = new HashSet<>(topology);
    }

    public int[] getPorts(@NotNull final Set<String> topology) {
        String[] endpoints = topology.toArray(new String[topology.size()]);
        int[] ports = new int[topology.size()];
        for (int i = 0; i < endpoints.length; i++) {
            ports[i] = Integer.parseInt(endpoints[i].substring(endpoints[i].lastIndexOf(':') + 1));
        }
        return ports;
    }

    public String[] getHosts(@NotNull final Set<String> topology) {
        String[] endpoints = topology.toArray(new String[topology.size()]);
        String[] hosts = new String[topology.size()];
        for (int i = 0; i < endpoints.length; i++) {
            hosts[i] = endpoints[i].substring("http://".length(), endpoints[i].lastIndexOf(':'));
        }
        return hosts;
    }
}
