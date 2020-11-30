package Chord;

import FusionDs.fusionLinkedList;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

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

public class ChordNode implements ChordRMI, Callable<Response>, Serializable {
    static final long serialVersionUID=33L;

    static final int intervalStartColumn = 0;
    static final int intervalEndColumn = 1;
    static final int nidColumn = 2;

    public int m;
    public int nid;
    public Map<Integer, Integer> hm;
    public fusionLinkedList fingerTable;
    public int successor;
    public int predecessor;

    ReentrantLock mutex;
    Registry registry;
    ChordRMI stub;

    ConcurrentLinkedQueue<String> taskQ = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Integer> keyQ = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Integer> valQ = new ConcurrentLinkedQueue<>();

    public ChordNode(int m, int id) {
        this.m = m;
        this.nid = id;
        this.fingerTable = new fusionLinkedList(3);
        this.hm = new ConcurrentHashMap<>();
        this.successor = id;
        this.predecessor = id;
        this.mutex = new ReentrantLock();

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
            } else if (rmi.equals("RemoveKey")) {
                callReply = stub.removeKey(r);
            } else if (rmi.equals("MigrateKey")) {
                callReply = stub.migrateKey(r);
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
    public void initFingerTable(int ServerId) throws Exception {
        for (int i = 1; i <= m; i++) {
            int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);;
            int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);;
            fingerTable.insert(intervalStartColumn, i, intervalStart);
            fingerTable.insert(intervalEndColumn, i, intervalEnd);
            fingerTable.insert(nidColumn, i, this.nid);
        }

        int successor = (Integer) Call("FindSuccessor", new Request(fingerTable.get(intervalStartColumn, 1)), ServerId).value; //Call("FindSuccessor", fingerTable[1].start, n.nid).node;

        fingerTable.insert(nidColumn, 1, successor);
        this.predecessor = (Integer) Call("GetPredecessor", new Request(-1), successor).value;
        this.successor = successor;

        int sucPredecessor = (Integer) Call("GetPredecessor", new Request(-1), successor).value;
        Call("SetSuccessor", new Request(this.nid), sucPredecessor);
        Call("SetPredecessor", new Request(this.nid), successor);
        if ((Integer) Call("GetSuccessor", new Request(-1), successor).value == successor) {
            Call("SetSuccessor", new Request(this.nid), successor);
        }
        for (int i = 1; i < m; i++) {
            if (inInterval(fingerTable.get(intervalStartColumn, i+1), nid, fingerTable.get(nidColumn, i))) {
                Integer iNid = fingerTable.get(nidColumn, i);
                fingerTable.insert(nidColumn, i+1, iNid);
            } else {
                Integer suc = (Integer) findSuccessor(new Request(fingerTable.get(intervalStartColumn, i+1))).value;
                fingerTable.insert(nidColumn, i+1, suc);
            }
        }
    }

    public Response updateFingerTable(Request r) throws Exception {
        int ChordId = r.ChordId;
        int i = r.fingerIndex;
        int startInterval = nid;
        Integer iNid = fingerTable.get(nidColumn, i);
        int endInterval = iNid < nid ? iNid + (int) Math.pow(2, m) : iNid;
        int modChordId = iNid < nid && ChordId < nid ? ChordId + (int) Math.pow(2, m) : ChordId;
        if ((nid == iNid) || (startInterval < modChordId && modChordId < endInterval)) {
            fingerTable.insert(nidColumn, i, ChordId);
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
            mutex.lock();
            try {
                this.hm.put(key, value);
                return new Response(value, target);
            } finally {
                mutex.unlock();
            }
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

    public void updateLocalKey() {
        Response rsp = Call("MigrateKey", new Request(this.nid), this.successor);
        Map<Integer,Integer> migrateMap = rsp.hm;
        for (Integer key : migrateMap.keySet()) {
            this.hm.put(key, migrateMap.get(key));
        }
    }

    public Response removeKey(Request r) {
        int key = r.key;
        int target = (Integer) Call("FindSuccessor", new Request(key), this.nid).value;
        if (target == this.nid) {
            if (this.hm.containsKey(key)) {
                int value = hm.get(key);
                hm.remove(key);
                return new Response(value, target);
            } else {
                return new Response(null, target);
            }
        }
        return Call("RemoveKey", new Request(target, key, null), target);
    }

    public Response migrateKey(Request r) {
        int ChordId = r.ChordId;
        Map<Integer, Integer> migrateMap = new HashMap<>();
        for (Integer key : hm.keySet()) {
            if (key % Math.pow(2, m) <= ChordId) {
                migrateMap.put(key, hm.get(key));
            }
        }

        for (Integer key : migrateMap.keySet()) {
            hm.remove(key);
        }

        return new Response(new HashMap<>(migrateMap));
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

    public Response findClosestPrecedingFinger(Request r) throws Exception {
        int id = r.ChordId;
        for (int i = m; i >= 1; i--) {
            Integer fingerId = fingerTable.get(nidColumn, i);
//            cur.nid < fingerId && fingerId < id
            if (inInterval(fingerId,nid,id)) {
                return new Response(fingerTable.get(nidColumn, i));
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

    public void join(ChordNode network) throws Exception {
        // there are no nodes in the entire network
        if (network == null) {
            predecessor=this.nid;
            successor=this.nid;
            for (int i = 1; i <= m; i++) {
                int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);
                int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);
                fingerTable.insert(intervalStartColumn, i, intervalStart);
                fingerTable.insert(intervalEndColumn, i, intervalEnd);
                fingerTable.insert(nidColumn, i, this.nid);
            }
        } else {
            initFingerTable(network.nid);
            updateOthers();
            updateLocalKey();
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

    public Future<Response> Start(String task, Integer key, Integer val) throws InterruptedException {
        mutex.lock();
        Future<Response> future;
        try {
            this.taskQ.add(task);
            this.keyQ.add(key);
            this.valQ.add(key);
        } finally {
            mutex.unlock();
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        future = executorService.submit(this);
        return future;
    }

    @Override
    public Response call() {
        mutex.lock();

        String curTask;
        Integer curKey;
        Integer curVal;

        try {
            curTask = this.taskQ.poll();
            curKey = this.keyQ.poll();
            curVal = this.valQ.poll();
        } finally {
            mutex.unlock();
        }

        Response rsp = new Response(null);

        if (curTask == null) return null;

        if (curTask.equals("PUT")) {
            rsp = putKey(new Request(this.nid, curKey, curVal));
        }
        if (curTask.equals("GET")) {
            rsp = getKey(new Request(this.nid, curKey, null));
        }
        return rsp;
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
