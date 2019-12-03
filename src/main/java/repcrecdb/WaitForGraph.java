package repcrecdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class WaitForGraph {
    private HashMap<String, HashSet<String>> graph; // waitForGraph

    private HashSet<String> visited; // visited node set for deadlock detect
    private HashSet<String> searched; // searched node set for deadlock detect
    private ArrayList<String> cycle;
    private boolean recordSwitch = false;

    public WaitForGraph() {
        graph = new HashMap<>();
    }

    public void addEdge(String source, String end) {
        HashSet<String> list = graph.getOrDefault(source, new HashSet<>());
        list.add(end);
        graph.put(source, list);
    }

    public void removeNode(String source) {
        graph.remove(source);
        for (String src : graph.keySet()) {
            graph.get(src).remove(source);
        }
    }

    public ArrayList<String> detectDeadlock() {
        searched = new HashSet<>();
        visited = new HashSet<>();
        for (String src : graph.keySet()) {
            if (!searched.contains(src)) {
                visited.add(src);
                if (hasLoopDFS(src)) {
                    return cycle;
                }
            }
        }
        return null;
    }

    private boolean hasLoopDFS(String source) {
        HashSet<String> list = graph.getOrDefault(source,  new HashSet<>());
        for (String end : list) {
            if (searched.contains(end)) {
                continue;
            }
            if (visited.contains(end)) {
                recordSwitch = true;
                cycle = new ArrayList<>();
                cycle.add(end);
                return true;
            }
            visited.add(end);
            if (hasLoopDFS(end)) {
                if (cycle.contains(end)) {
                    recordSwitch = false;
                }
                if (recordSwitch) {
                    cycle.add(end);
                }
                return true;
            }
        }
        searched.add(source);
        return false;
    }
}