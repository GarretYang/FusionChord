import java.util.ArrayList;
import java.util.List;

class Finger {
    public int start;
    public int[] interval;
    public ChordNode node;
    
    public Finger(int start, int[] interval, ChordNode node) {
        this.start = start;
        this.interval = interval;
        this.node = node;
    }

    public String toString() {
        return "start: " + start + " ,interval: [" + interval[0] + ", " + interval[1] + "), node: " + node.toString();
    }
}

public class ChordNode {
    public int m;
    public int nid;
    public List<Integer> keys;
    public Finger[] fingerTable;
    public ChordNode successor;
    public ChordNode predecessor;

    public ChordNode(int m, int id) {
        this.m = m;
        this.nid = id;
        this.fingerTable = new Finger[m+1];
        this.keys = new ArrayList<>();
        this.successor = this;
        this.predecessor = this;
    }

    // init finger table all nodes in the table points to the current node
    public void initFingerTable(ChordNode n) {
        for (int i = 1; i <= m; i++) {
            int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
            int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
            fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this);
        }
        ChordNode successor = n.findSuccessor(fingerTable[1].start);
        fingerTable[1].node = successor;
        this.predecessor = successor.predecessor;
        this.successor = successor;
        successor.predecessor = this;
        for (int i = 1; i < m; i++) {
            if (inInterval(fingerTable[i+1].start, nid, fingerTable[i].node.nid)) {
                fingerTable[i+1].node = fingerTable[i].node;
            } else {
                fingerTable[i+1].node = findSuccessor(fingerTable[i+1].start);
            }
        }
    }

    public void updateFingerTable(ChordNode s, int i) {
        if (inInterval(s.nid, nid, fingerTable[i].node.nid)) {
            fingerTable[i].node = s;
            predecessor.updateFingerTable(s, i);
        }
    }

    public void updateOthers() {
        for (int i = 1; i <= m; i++) {
            ChordNode pre = findPredecessor((nid-(int) Math.pow(2,i-1)) % (int) Math.pow(2,m));
            pre.updateFingerTable(this, i);
        }
    }

    public ChordNode addKey(int key) {
        int modKey = key % (int) Math.pow(2, m);
        ChordNode successor = findSuccessor(modKey);
        successor.keys.add(key);
        return successor;
    }

    public ChordNode findKey(int id) {
        int modId = id % (int) Math.pow(2,m);
        return findSuccessor(modId);
    }

    public ChordNode findSuccessor(int id) {
        if (successor.equals(this)) return this;
        if (nid == id) return this;
        ChordNode predecessor = findPredecessor(id);
        return predecessor.successor;
    }

    public ChordNode findPredecessor(int id) {
        ChordNode cur = this;
        ChordNode immediateSuccessor = cur.successor;
        while (!inInterval(id,cur.nid, immediateSuccessor.nid)) {
            cur = cur.findClosestPrecedingFinger(id);
            immediateSuccessor = cur.successor;
        }
        return cur;
    }

    public ChordNode findClosestPrecedingFinger(int id) {
        for (int i = m; i >= 1; i--) {
            int fingerId = fingerTable[i].node.nid;
            if (inInterval(fingerId,nid, id)) {
                return fingerTable[i].node;
            }
        }
        return this;
    }

    public boolean inInterval(int id, int start, int end) {
        if (start > end) {
            end += (int) Math.pow(2, m);
        }
        return start < id && id <= end;
    }

    public ChordNode join(ChordNode network) {
        // there are no nodes in the entire network
        if (network == null) {
            predecessor=this;
            successor=this;
            for (int i = 1; i <= m; i++) {
                int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
                int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
                fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this);
            }
        } else {
            initFingerTable(network);
            System.out.println("After init");
            updateOthers();
        }

        return this;
    }

    public String toString() {
        return this.nid + "";
    }

}
