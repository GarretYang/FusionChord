package Chord;

import FusionDs.backupLinkedList;
import FusionDs.fusionMatrix;
import FusionDs.primaryLinkedList;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    public int m;
    public int nid;
    public Map<Integer, Integer> hm;
    public Finger[] fingerTable;
    public int successor;
    public int predecessor;

    ReentrantLock mutex;
    Registry registry;
    ChordRMI stub;

    ConcurrentLinkedQueue<String> taskQ;
    ConcurrentLinkedQueue<Integer> keyQ;
    ConcurrentLinkedQueue<Integer> valQ;

    String NodeType;
    primaryLinkedList storageList;
    backupLinkedList backupList;

    int[][] preCrashData;

    public ChordNode(int m, int id) {
        this.m = m;
        this.nid = id;
        this.fingerTable = new Finger[m+1];
        this.hm = new ConcurrentHashMap<>();
        this.successor = id;
        this.predecessor = id;
        this.mutex = new ReentrantLock();
        this.taskQ = new ConcurrentLinkedQueue<>();
        this.keyQ = new ConcurrentLinkedQueue<>();
        this.valQ = new ConcurrentLinkedQueue<>();

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

    public ChordNode(int m, int id, String nodeType, int n, int f, int relativeOrder) {
        this(m, id);
        if (nodeType.equals("Storage")) {
            fusionMatrix fm = new fusionMatrix(n, f);
            this.NodeType = "Storage";
            this.storageList = new primaryLinkedList(fm.matrix, relativeOrder);
        } else if (nodeType.equals("BackUp")) {
            fusionMatrix fm = new fusionMatrix(n, f);
            this.NodeType = "BackUp";
            this.backupList = new backupLinkedList(n, fm.matrix, relativeOrder);
        }
    }

    public Response Call(String rmi, Request r, int serverId) {
        Response callReply = null;

        ChordRMI stub;
        try {
            int portId = serverId + 1000;
            mutex.lock();
            Registry registry = LocateRegistry.getRegistry(portId);
            stub=(ChordRMI) registry.lookup("Chord");
            mutex.unlock();
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
            } else if (rmi.equals("GetStorageData")) {
                callReply = stub.getStorageData();
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
            int intervalStart = (nid + (int) Math.pow(2, i-1)) % (int) Math.pow(2,m);
            int intervalEnd = (nid + (int) Math.pow(2, i)) % (int) Math.pow(2,m);
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

    public void requestStorageData(int[] ChordIds) {
        int maxLen = 0;
        List<Response> rspList = new ArrayList<>();

        for (int ChordId : ChordIds) {
            ChordId = (int) (ChordId % Math.pow(2, m));
            Response dataRsp = Call("GetStorageData", null, ChordId);
            rspList.add(dataRsp);
            maxLen = Math.max(maxLen, dataRsp.recoverData.length);
        }

        this.preCrashData = new int[ChordIds.length][maxLen];
        for (int i = 0; i < rspList.size(); i++) {
            Response rsp = rspList.get(i);
            for (int j = 0; j < rsp.recoverData.length; j++) {
                this.preCrashData[i][j] = rsp.recoverData[j];
            }
        }
    }

    public void crashAndRecover(int[] ChordIds) {
        byte[][] val = new byte[preCrashData.length][preCrashData[0].length];
        for (int i = 0; i < val.length; i++) {
            for (int j = 0; j < val[0].length; j++) {
                val[i][j] = (byte) this.preCrashData[i][j];
            }
        }
        this.storageList.crashAndRecover(ChordIds, val);
    }

    public Response getStorageData() {
        int[] storageData = new int[0];
        if ("Storage".equals(this.NodeType)) {
            storageData = this.storageList.get_data();
        } else if ("BackUp".equals(this.NodeType)) {
            storageData = this.backupList.get_recovery();
        }
        return new Response(storageData, this.nid);
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
    public Response notify(Request r) {
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
        String backup = this.backupList != null ? " (BackUp Node)" : "";
        String normal = this.storageList != null ? " (Storage Node)" : "";
        return "Node" + this.nid + backup + normal;
    }

    public Future<Response> Start(String task, Integer key, Integer val, ExecutorService executorService) {
        mutex.lock();
        try {
            this.taskQ.add(task);
            this.keyQ.add(key);
            this.valQ.add(val);
        } finally {
            mutex.unlock();
        }
        return executorService.submit(this);
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
//            System.out.println("key: " + curKey + ", value: " + rsp.value);
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

    public boolean exit() {
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
