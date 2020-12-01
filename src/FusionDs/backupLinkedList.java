package FusionDs;

import java.util.LinkedList;
import java.util.List;

public class backupLinkedList {
    Backup backup;
    static Matrix matrix;
    int j;
    int n;

    public backupLinkedList(int n, Matrix matrix, int j) {
        this.n = n;
        backup = new Backup(n);
        backupLinkedList.matrix = matrix;
        this.j = j;
    }

    public void insert(int i, int k, int old, int d) {
        AuxNode node = findAuxNode(k, backup.auxNodesList[i]);
        if (node != null) {
            FuseNode fuse = node.fuseNode;
            fuse.updateNode(i, old, d);
        } else {
            int stackId = backup.tos[i]++;
            if (stackId == backup.stack.size()) {
                FuseNode fuse = new FuseNode(this.n);
                backup.stack.add(fuse);
            }
            FuseNode p = backup.stack.get(stackId);
            p.updateNode(i, old, d);
            p.refCount++;
            AuxNode a = new AuxNode();
            a.fuseNode = p;
            a.key = k;
            p.auxNodes[i] = a;
            insertAuxNode(a, backup.auxNodesList[i]);
        }
    }

    public int[] get_recovery() {
        int[] res = new int[backup.stack.size()];
        for(int i=0; i<backup.stack.size(); i++) {
            res[i] = backup.stack.get(i).value;
        }
        return res;
    }

    AuxNode findAuxNode(int k, AuxNode startNode) {
        AuxNode node = startNode.next;
        while (node != null) {
            if (node.key == k) {
                return node;
            }
            node = node.next;
        }
        return null;
    }

    void insertAuxNode(AuxNode newNode, AuxNode startNode) {
        while(startNode.next!=null) {
            startNode = startNode.next;
        }
        startNode.next = newNode;
    }

    private class Backup {
        List<FuseNode> stack;
        AuxNode[] auxNodesList;
        int[] tos;

        public Backup(int n) {
            stack = new LinkedList<>();
            auxNodesList = new AuxNode[n];
            tos = new int[n];
            for (int i =0; i<n; i++) {
                auxNodesList[i] = new AuxNode();
                tos[i] = 0;
            }
        }
    }

    private class FuseNode {
        int value;
        int refCount;
        AuxNode[] auxNodes;

        public FuseNode(int n) {
            value = 0;
            refCount = 0;
            auxNodes = new AuxNode[n];
        }

        public void updateNode(int i, int old, int d) {
            value += matrix.get(j, i) * (d - old);
        }
    }

    private class AuxNode {
        FuseNode fuseNode;
        AuxNode next;
        int key;
        public AuxNode(){
            next = null;
            fuseNode = null;
        }
    }
}
