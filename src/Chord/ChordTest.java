package Chord;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class ChordTest {

    // the ring should be 0 - 1 - 3 - 6
    //                    \ - - - - - |
    public List<ChordNode> buildRing(boolean shouldPrint) {

        List<ChordNode> nodes = new ArrayList<>();

        ChordNode Node0 = new ChordNode(3,0);
        Node0.join(null);
        nodes.add(Node0);

        ChordNode Node3 = new ChordNode(3, 3);
        Node3.join(Node0);
        nodes.add(Node3);

        ChordNode Node1 = new ChordNode(3, 1);
        Node1.join(Node0);
        nodes.add(Node1);

        ChordNode Node6 = new ChordNode(3, 6);
        Node6.join(Node1);
        nodes.add(Node6);

        if (shouldPrint) {
            for (ChordNode node : nodes) {
                for (int i = 1; i <= node.m; i++) {
                    System.out.println(node.fingerTable[i]);
                }

                System.out.println(node + " predecessor: " + node.predecessor + ", " + node + " successor: " + node.successor);

                System.out.println("------------------");
            }
        }

        return nodes;
    }

    // build a whole circle from 0 - 6
    public List<ChordNode> buildFullRing(boolean shouldPrint) {

        List<ChordNode> nodes = new ArrayList<>();

        ChordNode Node0 = new ChordNode(3,0);
        Node0.join(null);
        nodes.add(Node0);

        ChordNode Node3 = new ChordNode(3, 3);
        Node3.join(Node0);
        nodes.add(Node3);

        ChordNode Node1 = new ChordNode(3, 1);
        Node1.join(Node0);
        nodes.add(Node1);

        ChordNode Node6 = new ChordNode(3, 6);
        Node6.join(Node1);
        nodes.add(Node6);

        ChordNode Node4 = new ChordNode(3, 4);
        Node4.join(Node1);
        nodes.add(Node4);

        ChordNode Node5 = new ChordNode(3, 5);
        Node5.join(Node0);
        nodes.add(Node5);

        ChordNode Node2 = new ChordNode(3, 2);
        Node2.join(Node0);
        nodes.add(Node2);

        ChordNode Node7 = new ChordNode(3, 7);
        Node7.join(Node5);
        nodes.add(Node7);

        nodes.sort(Comparator.comparing(ChordNode::toString));

        if (shouldPrint) {
            for (ChordNode node : nodes) {
                for (int i = 1; i <= node.m; i++) {
                    System.out.println(node.fingerTable[i]);
                }

                System.out.println(node + " predecessor: " + node.predecessor + ", " + node + " successor: " + node.successor);

                System.out.println("------------------");
            }
        }

        return nodes;
    }

    @Test
    public void TestJoin() {
        ChordNode Node0 = new ChordNode(3,0);
        Node0.join(null);

        assertEquals("successor and predecessor should be equal if there is only one node", Node0.successor, Node0.predecessor);

        ChordNode Node3 = new ChordNode(3, 3);
        Node3.join(Node0);

        assertEquals("Node 0 should have Node 3 as predecessor", Node0.predecessor, Node3.nid);
        assertEquals("Node 0 should have Node 3 as successor", Node0.successor, Node3.nid);

        assertEquals("Node 3 should have Node 0 as predecessor", Node3.predecessor, Node0.nid);
        assertEquals("Node 3 should have Node 0 as successor", Node3.successor, Node0.nid);

        Chord.ChordNode Node1 = new Chord.ChordNode(3, 1);
        Node1.join(Node3);

        assertEquals("Node 1 should have Node 0 as predecessor", Node1.predecessor, Node0.nid);
        assertEquals("Node 1 should have Node 3 as successor", Node1.successor, Node3.nid);

        assertEquals("Node 0 should have Node 1 as successor", Node0.successor, Node1.nid);
        assertEquals("Node 3 should have Node 1 as predecessor", Node3.predecessor, Node1.nid);

        try {
            Node0.exit();
            Node1.exit();
            Node3.exit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void TestFindSuccessor() {
        List<ChordNode> nodes = buildRing(false);

        // find successors of keys that are not in the ring
        for (ChordNode node : nodes) {
            assertEquals(node.findSuccessor(new Request(2)).value, 3);
            assertEquals(node.findSuccessor(new Request(4)).value, 6);
            assertEquals(node.findSuccessor(new Request(7)).value, 0);
        }

        // find successors of keys that are in the ring
        for (ChordNode node : nodes) {
            assertEquals(node.findSuccessor(new Request(0)).value, 0);
            assertEquals(node.findSuccessor(new Request(1)).value, 1);
            assertEquals(node.findSuccessor(new Request(3)).value, 3);
            assertEquals(node.findSuccessor(new Request(6)).value, 6);
        }

        for (ChordNode node : nodes) {
            try {
                node.exit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void TestFindPredecessor() {
        List<ChordNode> nodes = buildRing(false);

        // find the predecessor of keys that are not in the ring
        for (ChordNode node : nodes) {
            assertEquals(node.findPredecessor(new Request(2)).value, 1);
            assertEquals(node.findPredecessor(new Request(4)).value, 3);
            assertEquals(node.findPredecessor(new Request(7)).value, 6);
        }

        // find the predecessor of keys that are in the ring
        for (ChordNode node : nodes) {
            assertEquals(node.findPredecessor(new Request(0)).value, 6);
            assertEquals(node.findPredecessor(new Request(1)).value, 0);
            assertEquals(node.findPredecessor(new Request(3)).value, 1);
            assertEquals(node.findPredecessor(new Request(6)).value, 3);
        }

        for (ChordNode node : nodes) {
            try {
                node.exit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void TestBuildFullRing() {
        buildFullRing(true);
    }

    @Test
    public void TestKeyAllocation() {

        // Node 0, 1, 3, 6
        List<ChordNode> nodes = buildRing(false);
        Random r = new Random();

        ChordNode rNode1 = nodes.get(r.nextInt(nodes.size()));
        ChordNode rNode2 = nodes.get(r.nextInt(nodes.size()));

        rNode1.putKey(new Request(null, 4, 4444));
        rNode1.putKey(new Request(null, 20, 2020));
        Response r1 = rNode2.getKey(new Request(null, 4, null));
        Response r2 = rNode2.putKey(new Request(null, 20, 2020));
        Response r3 = rNode2.getKey(new Request(null, 0, null));

        assertEquals("The value should be 4444", 4444, r1.value);
        assertEquals("The value should be 2020", 2020, r2.value);
        assertNull("The value should not exist", r3.value);
        assertEquals("The server should be 6", 6, r1.ChordId);
        assertEquals("The server should be 6", 6, r2.ChordId);

    }
}
