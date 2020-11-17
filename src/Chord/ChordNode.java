import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
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

public class ChordNode implements ChordRMI, Runnable, Serializable {
    static final long serialVersionUID=33L;
    public int m;
    public int nid;
    public List<Integer> keys;
    public Finger[] fingerTable;
    public ChordNode successor;
    public ChordNode predecessor;

    Registry registry;
    ChordRMI stub;

    public ChordNode(int m, int id) {
        this.m = m;
        this.nid = id;
        this.fingerTable = new Finger[m+1];
        this.keys = new ArrayList<>();
        this.successor = this;
        this.predecessor = this;

        try {
            System.setProperty("java.rmi.server.hostname", "127.0.0.1");
            int portId = this.nid + 1000;
            registry = LocateRegistry.createRegistry(portId);
            stub = (ChordRMI) UnicastRemoteObject.exportObject(this, portId);
            registry.rebind("Chord", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Response Call(String rmi, int ChordId, int serverId) {
        Response callReply = null;

        ChordRMI stub;
        try {
            int portId = serverId + 1000;
            Registry registry = LocateRegistry.getRegistry(portId);
            stub=(ChordRMI) registry.lookup("Chord");
            if (rmi.equals("FindSuccessor")) {
                callReply = stub.findSuccessor(new Request(ChordId));
            } else if (rmi.equals("FindPredecessor")) {
//                callReply = stub.findPredecessor(ChordId);
            } else if (rmi.equals("Notify")) {
                callReply = stub.notify(new Request(ChordId));
            } else {
                System.out.println("Invalid parameters");
            }
        } catch (Exception e) {
            return null;
        }

        return callReply;
    }

    // init finger table all nodes in the table points to the current node
    public void initFingerTable(ChordNode n) {
        for (int i = 1; i <= m; i++) {
            int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
            int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
            fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this);
        }
        ChordNode successor = Call("FindSuccessor", fingerTable[1].start, n.nid).node;
        fingerTable[1].node = successor;
        this.predecessor = successor.predecessor;
        this.successor = successor;
        successor.predecessor.successor = this;
        successor.predecessor = this;
        if (successor.successor.equals(successor)) {
            successor.successor = this;
        }
        for (int i = 1; i < m; i++) {
            if (inInterval(fingerTable[i+1].start, nid, fingerTable[i].node.nid)) {
                fingerTable[i+1].node = fingerTable[i].node;
            } else {
                fingerTable[i+1].node = findSuccessor(new Request(fingerTable[i+1].start)).node;
            }
        }
    }

    public void updateFingerTable(ChordNode s, int i) {
        int startInterval = nid;
        int endInterval = fingerTable[i].node.nid < nid ? fingerTable[i].node.nid + (int) Math.pow(2, m) : fingerTable[i].node.nid;
        if ((nid == fingerTable[i].node.nid) || (startInterval < s.nid && s.nid < endInterval)) {
            fingerTable[i].node = s;
            predecessor.updateFingerTable(s, i);
        }
    }

    public void updateOthers() {
        for (int i = 1; i <= m; i++) {
            int pid = (nid-(int) Math.pow(2,i-1)) % (int) Math.pow(2,m);
            ChordNode pre = findPredecessor(pid);
            if (pre.successor.nid == pid) pre = pre.successor;
            pre.updateFingerTable(this, i);
        }
    }

    public ChordNode addKey(int key) {
        int modKey = key % (int) Math.pow(2, m);
        ChordNode successor = findSuccessor(new Request(modKey)).node;
        successor.keys.add(key);
        return successor;
    }

    public ChordNode findKey(int id) {
        int modId = id % (int) Math.pow(2, m);
        return findSuccessor(new Request(modId)).node;
    }

    public Response findSuccessor(Request r) {
        int id = r.ChordId;
        if (successor.equals(this)) return new Response(this);
        if (nid == id) return new Response(this);
        ChordNode predecessor = findPredecessor(id);
        return new Response(predecessor.successor);
    }

    public ChordNode findPredecessor(int id) {
        ChordNode cur = this;
        ChordNode immediateSuccessor = cur.successor;

        if (id == cur.nid) return cur.predecessor;

        while (!inInterval(id,cur.nid, immediateSuccessor.nid)) {
            if (id == cur.nid) return cur.predecessor;
            cur = cur.findClosestPrecedingFinger(id);
            immediateSuccessor = cur.successor;
        }
        return cur;
    }

    @Override
    public Response notify(Request r) throws RemoteException {
        return null;
    }

    public ChordNode findClosestPrecedingFinger(int id) {
        for (int i = m; i >= 1; i--) {
            int fingerId = fingerTable[i].node.nid;
//            cur.nid < fingerId && fingerId < id
            if (inInterval(fingerId,nid,id)) {
                return fingerTable[i].node;
            }
        }
        return this;
    }

    public boolean inInterval(int id, int start, int end) {
        if (start >= end) {
            if (id <= end) return true;
            end += (int) Math.pow(2, m);
        }
        return start < id && id <= end;
    }

    public void join(ChordNode network) {
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
            updateOthers();
        }
    }

    public synchronized void concurrentJoin(ChordNode network) {
        if (network == null) {
            join(null);
        } else {
            for (int i = 1; i <= m; i++) {
                int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
                int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
                fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this);
            }
            ChordNode successor = network.findSuccessor(new Request(fingerTable[1].start)).node;
            fingerTable[1].node = successor;
            this.successor = successor;
        }
    }

    public String toString() {
        return this.nid + "";
    }

    @Override
    public void run() {

    }
}
