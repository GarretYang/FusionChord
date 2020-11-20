package FusionDs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class fusionLinkedList {
    primary[] primaryArray;
    backup[] backupArray;
    int N;

    public fusionLinkedList(int n) {
        n = 3; N =3;
        primaryArray = new primary[n];
        for(int i=0; i<n; i++) {
            primaryArray[i] = new primary();
        }

        backupArray = new backup[n];
        for(int i=0; i<n; i++) {
            backupArray[i] = new backup(n);
        }
    }

    public Integer get(int i, int k) throws Exception {
        primListNode node = findPrimNode(k, primaryArray[i].sPrimNode);
        if (node==null) {
            return null;
        }
        return node.value;
    }

    public void insert(int i, int k, int d) {
        int old;
        primListNode node = findPrimNode(k, primaryArray[i].sPrimNode);
        if (node != null) {
            old = node.value;
            node.value = d;
            for(int j = 0; j<N; j++) {
                insert_backup(j, k, d, old, i);
            }
        } else {
            primListNode newNode = new primListNode();
            newNode.value = d;
            newNode.key = k;
            auxListNode newAuxNode = new auxListNode();
            newAuxNode.primNode = newNode;
            newAuxNode.key = k;
            newNode.auxNode = newAuxNode;
            insertAuxNode(newAuxNode, primaryArray[i].sAuxNode);
            insertPrimNode(newNode, primaryArray[i].sPrimNode);
            // update
            old = 0;
            for(int j = 0; j<N; j++) {
                insert_backup(j, k, d, old, i);
            }
        }
    }

    protected void insert_backup(int j, int k, int d_i, int old_i, int i) {
        auxListNode node = findAuxNode(k, backupArray[j].auxListArray[i]);
        if (node !=null) {
            fuseNode fuse = node.fuse;
            fuse.updateNode(i, old_i, d_i);
        } else {
            int stackId = backupArray[j].tos[i]++;
            if (stackId == backupArray[j].stack.size()) {
                fuseNode fuse = new fuseNode(N, j);
                backupArray[j].stack.add(fuse);
            }
            fuseNode p = backupArray[j].stack.get(stackId);
            p.updateNode(i, old_i, d_i);
            p.refCount++;
            auxListNode a = new auxListNode();
            a.fuse = p;
            a.key = k;
            p.auxNodesList[i] = a;
            insertAuxNode(a, backupArray[j].auxListArray[i]);
        }
    }

    public void delete(int i, int k) {

    }

    protected void delete_backup(int j, int k, int old_i, int tos_i){

    }

    public void crashAndRecover(int i) {
        int[][] recover_sign = {
                {1, 1, 0},
                {1, 0, -1},
                {0, 1, -1}
        };
        primListNode node = primaryArray[i].sPrimNode;
        System.out.println("before crash, primary storage no." + i + " values: ");
        node = node.next;
        while (node != null) {
         System.out.println("k :" + node.key + ", v: " + node.value);
         node = node.next;
        }

        primaryArray[i].sPrimNode.next = null;
        primaryArray[i].sAuxNode.next = null;
        System.out.println("crashed, recovering...");

        int size = backupArray[0].tos[i];
        for (int q =0; q<size; q++) {
            int k=0;
            int val = 0;
            for (int j=0; j<N; j++) {
                fuseNode fuse = backupArray[j].stack.get(q);
                val += recover_sign[i][j] * fuse.value;
                k = fuse.auxNodesList[i].key;
            }
            val /= 2;
            primListNode newNode = new primListNode();
            newNode.key = k;
            newNode.value = val;
            auxListNode newAuxNode = new auxListNode();
            newAuxNode.primNode = newNode;
            newNode.auxNode = newAuxNode;
            insertAuxNode(newAuxNode, primaryArray[i].sAuxNode);
            insertPrimNode(newNode, primaryArray[i].sPrimNode);
        }

        System.out.println("recovered, primary storage no." + i + " values: ");
        node = primaryArray[i].sPrimNode.next;
        while (node != null) {
            System.out.println("k :" + node.key + ", v: " + node.value);
            node = node.next;
        }
    }

    public static void main(String[] args) throws Exception {
        fusionLinkedList fll = new fusionLinkedList(3);
        fll.insert(0, 2, 1);
        fll.insert(0, 5, 1);
        fll.insert(1, 1, 1);
        fll.insert(1, 4, 1);
        fll.insert(1, 6, 1);
        int res = fll.get(1, 6);
        System.out.println(res);
        fll.crashAndRecover(1);
    }

    primListNode findPrimNode(int k, primListNode startNode) {
        primListNode node = startNode.next;
        while (node != null) {
            if (node.key == k) {
                return node;
            }
            node = node.next;
        }
        return null;
    }

//    primListNode deletePrimNode(int k, primListNode startNode) {
//
//    }

    auxListNode findAuxNode(int k, auxListNode startNode) {
        auxListNode node = startNode.next;
        while (node != null) {
            if (node.key == k) {
                return node;
            }
            node = node.next;
        }
        return null;
    }

    void insertAuxNode(auxListNode newNode, auxListNode startNode) {
        while(startNode.next!=null) {
            startNode = startNode.next;
        }
        startNode.next = newNode;
    }

    void insertPrimNode(primListNode newNode, primListNode startNode) {
        while(startNode.next!=null) {
            startNode = startNode.next;
        }
        startNode.next = newNode;
    }
}

class backup {
    List<fuseNode> stack;
    auxListNode[] auxListArray;
    int[] tos;
    public backup (int n) {
        stack = new ArrayList<>();
        tos = new int[n];
        auxListArray = new auxListNode[n];
        for(int i=0; i<n; i++) {
            auxListArray[i] = new auxListNode();
            tos[i] = 0;
        }
    }
}

class primary {
    primListNode sPrimNode;
    auxListNode sAuxNode;
    public primary() {
        sPrimNode = new primListNode();
        sAuxNode = new auxListNode();
    }
}

class sign {
    static int[][] sign = {
            {1, 1, -1},
            {1, -1, 1},
            {1, -1, -1}
    };
    static int getSign(int j, int i) {
        return sign[j][i];
    }
}

class fuseNode {
    int value;
    int refCount;
    int j;
    auxListNode[] auxNodesList;

    public fuseNode(int n, int j_) {
        value = 0;
        refCount = 0;
        auxNodesList = new auxListNode[n];
        j = j_;
    }

    public void updateNode(int i, Integer old, int d) {
        int sign = FusionDs.sign.getSign(j, i);
        value += sign * (d - old);
    }
}

class primListNode {
    int key;
    int value;
    auxListNode auxNode;
    primListNode next;
    public primListNode(){
        auxNode = null;
        next = null;
    }
}

class auxListNode {
    primListNode primNode;
    auxListNode next;
    fuseNode fuse;
    int key;
    public auxListNode(){
        fuse = null;
        next = null;
        primNode = null;
        key = -1;
    }
}