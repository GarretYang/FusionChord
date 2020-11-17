import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
            for (int i = 1; i <= Node0.m; i++) {
                System.out.println(Node0.fingerTable[i]);
            }
            System.out.println("Node0 predecessor: " + Node0.predecessor + ", Node0 successor: " + Node0.successor);

            System.out.println("------------------");

            for (int i = 1; i <= Node0.m; i++) {
                System.out.println(Node1.fingerTable[i]);
            }

            System.out.println("Node1 predecessor: " + Node1.predecessor + ", Node1 successor: " + Node1.successor);

            System.out.println("------------------");

            for (int i = 1; i <= Node0.m; i++) {
                System.out.println(Node3.fingerTable[i]);
            }

            System.out.println("Node3 predecessor: " + Node3.predecessor + ", Node3 successor: " + Node3.successor);

            System.out.println("------------------");

            for (int i = 1; i <= Node0.m; i++) {
                System.out.println(Node6.fingerTable[i]);
            }

            System.out.println("Node6 predecessor: " + Node6.predecessor + ", Node6 successor: " + Node6.successor);
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
//
//        assertEquals("Node 0 should have Node 3 as predecessor", Node0.predecessor, Node3);
//        assertEquals("Node 0 should have Node 3 as successor", Node0.successor, Node3);
//
//        assertEquals("Node 3 should have Node 0 as predecessor", Node3.predecessor, Node0);
//        assertEquals("Node 3 should have Node 0 as successor", Node3.successor, Node0);
//
//        ChordNode Node1 = new ChordNode(3, 1);
//        Node1.join(Node3);
//
//        assertEquals("Node 1 should have Node 0 as predecessor", Node1.predecessor, Node0);
//        assertEquals("Node 1 should have Node 3 as successor", Node1.successor, Node3);
//
//        assertEquals("Node 0 should have Node 1 as successor", Node0.successor, Node1);
//        assertEquals("Node 3 should have Node 1 as predecessor", Node3.predecessor, Node1);
    }

    @Test
    public void TestFindSuccessor() {
        List<ChordNode> nodes = buildRing(false);

        // find successors of keys that are not in the ring
        for (ChordNode node : nodes) {
//            assertEquals(node.findSuccessor(2).nid, 3);
//            assertEquals(node.findSuccessor(4).nid, 6);
//            assertEquals(node.findSuccessor(7).nid, 0);
        }

        // find successors of keys that are in the ring
        for (ChordNode node : nodes) {
//            assertEquals(node.findSuccessor(0).nid, 0);
//            assertEquals(node.findSuccessor(1).nid, 1);
//            assertEquals(node.findSuccessor(3).nid, 3);
//            assertEquals(node.findSuccessor(6).nid, 6);
        }
    }

    @Test
    public void TestFindPredecessor() {
        List<ChordNode> nodes = buildRing(false);

        // find the predecessor of keys that are not in the ring
        for (ChordNode node : nodes) {
            assertEquals(node.findPredecessor(2).nid, 1);
            assertEquals(node.findPredecessor(4).nid, 3);
            assertEquals(node.findPredecessor(7).nid, 6);
        }

        // find the predecessor of keys that are in the ring
        for (ChordNode node : nodes) {
            assertEquals(node.findPredecessor(0).nid, 6);
            assertEquals(node.findPredecessor(1).nid, 0);
            assertEquals(node.findPredecessor(3).nid, 1);
            assertEquals(node.findPredecessor(6).nid, 3);
        }
    }

}
