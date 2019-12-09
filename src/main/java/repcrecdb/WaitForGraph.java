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

    /**
     * Description: initialize wait for graph
     * Input: N/A
     * Output: N/A
     */
    public WaitForGraph() {
        graph = new HashMap<>();
    }

    /**
     * Description: add one edge to wait for graph
     * Input: edge source and edge end
     * Output: N/A
     * Side effect: add edge to wait for graph
     */
    public void addEdge(String source, String end) {
        HashSet<String> list = graph.getOrDefault(source, new HashSet<>());
        list.add(end);
        graph.put(source, list);
    }

    /**
     * Description: remove all edges connected to some node
     * Input: node name
     * Output: N/A
     * Side effect: all edges connected to this node will be removed
     */
    public void removeNode(String source) {
        graph.remove(source);
        for (String src : graph.keySet()) {
            graph.get(src).remove(source);
        }
    }

    /**
     * Description: detect deadlock in current wait for graph
     * Input: N/A
     * Output: list containing all nodes in a loop, or empty list if no loop
     * Side effect:
     * Initialize hashset searched and visited
     * Add nodes to set visited if visited
     */
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

    /**
     * Description: if loop exists start through DFS
     * Input: source node
     * Output: true is loop exists, false is not
     * Side effect: 
     * Add node to visited once visit one node
     * Add node to searched if all paths from this node has been visited
     * change recordSwitch to true if currently in loop, false if not
     * Add node to cycle if currently in a loop
     */
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