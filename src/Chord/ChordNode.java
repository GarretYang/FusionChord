package Chord;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Finger {
    public int start;
    public int[] interval;
    public int nid;
    
    public Finger(int start, int[] interval, int nid) {
        this.start = start;
        this.interval = interval;
        this.nid = nid;
    }

    public String toString() {
        return "start: " + start + " ,interval: [" + interval[0] + ", " + interval[1] + "), node: " + nid;
    }
}

public class ChordNode implements ChordRMI, Runnable, Serializable {
    static final long serialVersionUID=33L;
    public int m;
    public int nid;
    public Map<Integer, Integer> hm;
    public Finger[] fingerTable;
    public int successor;
    public int predecessor;

    Registry registry;
    ChordRMI stub;

    public ChordNode(int m, int id) {
        this.m = m;
        this.nid = id;
        this.fingerTable = new Finger[m+1];
        this.hm = new HashMap<>();
        this.successor = id;
        this.predecessor = id;

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

    public Response Call(String rmi, Request r, int serverId) {
        Response callReply = null;

        ChordRMI stub;
        try {
            int portId = serverId + 1000;
            Registry registry = LocateRegistry.getRegistry(portId);
            stub=(ChordRMI) registry.lookup("Chord");
            if (rmi.equals("FindSuccessor")) {
                callReply = stub.findSuccessor(r);
            } else if (rmi.equals("FindPredecessor")) {
                callReply = stub.findPredecessor(r);
            } else if (rmi.equals("Notify")) {
//                callReply = stub.notify(new Request(ChordId));
            } else if (rmi.equals("GetNID")) {
                callReply = stub.getNID();
            } else if (rmi.equals("GetSuccessor")) {
                callReply = stub.getSuccessor();
            } else if (rmi.equals("GetPredecessor")) {
                callReply = stub.getPredecessor();
            } else if (rmi.equals("SetSuccessor")) {
                callReply = stub.setSuccessor(r);
            } else if (rmi.equals("SetPredecessor")) {
                callReply = stub.setPredecessor(r);
            } else if (rmi.equals("FindClosestPrecedingFinger")) {
                callReply = stub.findClosestPrecedingFinger(r);
            } else if (rmi.equals("UpdateFingerTable")) {
                callReply = stub.updateFingerTable(r);
            } else if (rmi.equals("PutKey")) {
                callReply = stub.putKey(r);
            } else if (rmi.equals("GetKey")) {
                callReply = stub.getKey(r);
            } else {
                stub.addNode(new ChordNode(3, 1));
                System.out.println("Invalid parameters");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return callReply;
    }

    // init finger table all nodes in the table points to the current node
    public void initFingerTable(int ServerId) {
        for (int i = 1; i <= m; i++) {
            int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
            int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
            fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this.nid);
        }
        int successor = (Integer) Call("FindSuccessor", new Request(fingerTable[1].start), ServerId).value; //Call("FindSuccessor", fingerTable[1].start, n.nid).node;

        fingerTable[1].nid = successor;
        this.predecessor = (Integer) Call("GetPredecessor", new Request(-1), successor).value;
        this.successor = successor;

        int sucPredecessor = (Integer) Call("GetPredecessor", new Request(-1), successor).value;
        Call("SetSuccessor", new Request(this.nid), sucPredecessor);
        Call("SetPredecessor", new Request(this.nid), successor);
        if ((Integer) Call("GetSuccessor", new Request(-1), successor).value == successor) {
            Call("SetSuccessor", new Request(this.nid), successor);
        }
        for (int i = 1; i < m; i++) {
            if (inInterval(fingerTable[i+1].start, nid, fingerTable[i].nid)) {
                fingerTable[i+1].nid = fingerTable[i].nid;
            } else {
                fingerTable[i+1].nid = (Integer) findSuccessor(new Request(fingerTable[i+1].start)).value;
            }
        }
    }

    public Response updateFingerTable(Request r) {
        int ChordId = r.ChordId;
        int i = r.fingerIndex;
        int startInterval = nid;
        int endInterval = fingerTable[i].nid < nid ? fingerTable[i].nid + (int) Math.pow(2, m) : fingerTable[i].nid;
        int modChordId = fingerTable[i].nid < nid && ChordId < nid ? ChordId + (int) Math.pow(2, m) : ChordId;
        if ((nid == fingerTable[i].nid) || (startInterval < modChordId && modChordId < endInterval)) {
            fingerTable[i].nid = ChordId;
            Call("UpdateFingerTable", new Request(ChordId, i), this.predecessor);
        }
        return null;
    }

    public void updateOthers() {
        for (int i = 1; i <= m; i++) {
            int pid = (nid-(int) Math.pow(2,i-1)) % (int) Math.pow(2,m);
            int pre = (Integer) findPredecessor(new Request(pid)).value;
            int preSuccessor = (Integer) Call("GetSuccessor", null, pre).value;
            if (preSuccessor == pid) pre = preSuccessor;
            Call("UpdateFingerTable", new Request(this.nid, i), pre);
        }
    }

    public Response putKey(Request r) {
        int key = r.key;
        int value = r.value;
        int target = (Integer) Call("FindSuccessor", new Request(key), this.nid).value;
        if (target == this.nid) {
            this.hm.put(key, value);
            return new Response(value, target);
        }
        return Call("PutKey", new Request(target, key, value), target);
    }

    public Response getKey(Request r) {
        int key = r.key;
        int target = (Integer) Call("FindSuccessor", new Request(key), this.nid).value;
        if (target == this.nid) {
            if (this.hm.containsKey(key)) {
                return new Response(hm.get(key), target);
            } else {
                return new Response(null, target);
            }
        }
        return Call("GetKey", new Request(target, key, null), target);
    }

    public Response findSuccessor(Request r) {
        int ChordId = (int) (r.ChordId % Math.pow(2, m));
        if (successor == this.nid) return new Response(this.nid);
        if (this.nid == ChordId) return new Response(this.nid);
        int predecessor = (Integer) findPredecessor(new Request(ChordId)).value;
        return Call("GetSuccessor", null, predecessor);
    }

    public Response findPredecessor(Request r) {
        int id = (int) (r.ChordId % Math.pow(2, m));

        int cur = this.nid;
        int immediateSuccessor = this.successor;

        if (id == nid) return new Response(this.predecessor);

        while (!inInterval(id, cur, immediateSuccessor)) {
            if (id == cur) return Call("GetPredecessor", null, cur);

            cur = (Integer) Call("FindClosestPrecedingFinger", new Request(id), cur).value;

            immediateSuccessor = (Integer) Call("GetSuccessor", new Request(id), cur).value;
        }
        return new Response(cur);
    }

    @Override
    public Response notify(Request r) throws RemoteException {
        return null;
    }

    public Response findClosestPrecedingFinger(Request r) {
        int id = r.ChordId;
        for (int i = m; i >= 1; i--) {
            int fingerId = fingerTable[i].nid;
//            cur.nid < fingerId && fingerId < id
            if (inInterval(fingerId,nid,id)) {
                return new Response(fingerTable[i].nid);
            }
        }
        return new Response(this.nid);
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
            predecessor=this.nid;
            successor=this.nid;
            for (int i = 1; i <= m; i++) {
                int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);
                int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);
                fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this.nid);
            }
        } else {
            initFingerTable(network.nid);
            updateOthers();
        }
    }

    public synchronized void concurrentJoin(ChordNode network) {
//        if (network == null) {
//            join(null);
//        } else {
//            for (int i = 1; i <= m; i++) {
//                int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
//                int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
//                fingerTable[i] = new Finger(intervalStart, new int[]{intervalStart, intervalEnd}, this);
//            }
//            ChordNode successor = network.findSuccessor(new Request(fingerTable[1].start)).node;
//            fingerTable[1].node = successor;
//            this.successor = successor;
//        }
    }

    public String toString() {
        return "Node" + this.nid;
    }

    @Override
    public void run() {

    }

    public void callGetPort() {
//        Call("", 0, 0);
    }

    public int getPort() {
        return this.nid;
    }

    public void addNode(ChordNode node) {
        System.out.println(node);
    }


    public Response getNID() {
        return new Response(this.nid);
    }

    public Response getSuccessor() {
        return new Response(this.successor);
    }

    public Response getPredecessor() {
        return new Response(this.predecessor);
    }

    public Response setPredecessor(Request r) {
        this.predecessor = r.ChordId;
        return null;
    }

    public Response setSuccessor(Request r) {
        this.successor = r.ChordId;
        return null;
    }

    public boolean exit() throws RemoteException{
        try{
            int portId = this.nid + 1000;
            // Unregister ourself
            registry.unbind("Chord");

            // Unexport; this will also remove us from the RMI runtime
            UnicastRemoteObject.unexportObject(this, true);

            System.out.println("Server exited.");
            return true;
        }
        catch(Exception e){
            System.out.println("Server failed to exit.");
            return false;
        }
    }
}
