package FusionDs;

public class primaryLinkedList {
    Primary primary;

    public primaryLinkedList() {
        primary = new Primary();
    }

    public Integer get(int k) throws Exception {
        primListNode node = findPrimNode(k, primary.sPrimNode);
        if (node==null) {
            return null;
        }
        return node.value;
    }

    public Integer insert(int k, int d) {
        Integer old = null;
        primListNode node = findPrimNode(k, primary.sPrimNode);
        if (node != null) {
            old = node.value;
            node.value = d;
        } else {
            primListNode newNode = new primListNode();
            newNode.value = d;
            newNode.key = k;
            auxListNode newAuxNode = new auxListNode();
            newAuxNode.primNode = newNode;
            newAuxNode.key = k;
            newNode.auxNode = newAuxNode;
            insertAuxNode(newAuxNode, primary.sAuxNode);
            insertPrimNode(newNode, primary.sPrimNode);
        }
        return old;
    }

    public static void main(String[] args) throws Exception {
        primaryLinkedList fll = new primaryLinkedList();
        fll.insert(2, 1);
        assert fll.get(2) != null;
        fll.insert(5, 1);
        assert fll.get(5) != null;
        fll.insert(1, 1);
        assert fll.get(1) != null;
        fll.insert(4, 1);
        assert fll.get(4) != null;
        fll.insert(6, 1);
        assert fll.get(6) != null;

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

class Primary {
    primListNode sPrimNode;
    auxListNode sAuxNode;
    public Primary() {
        sPrimNode = new primListNode();
        sAuxNode = new auxListNode();
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
    int key;
    public auxListNode(){
        next = null;
        primNode = null;
    }
}

//class fuseNode {
//    int value;
//    int refCount;
//    int j;
//    auxListNode[] auxNodesList;
//
//    public fuseNode(int n, int j_) {
//        value = 0;
//        refCount = 0;
//        auxNodesList = new auxListNode[n];
//        j = j_;
//    }
//
//    public void updateNode(int i, Integer old, int d) {
//        int sign = FusionDs.sign.getSign(j, i);
//        value += sign * (d - old);
//    }
//}

//class backup {
//    List<fuseNode> stack;
//    auxListNode[] auxListArray;
//    int[] tos;
//    public backup (int n) {
//        stack = new ArrayList<>();
//        tos = new int[n];
//        auxListArray = new auxListNode[n];
//        for(int i=0; i<n; i++) {
//            auxListArray[i] = new auxListNode();
//            tos[i] = 0;
//        }
//    }
//}

//    protected void insert_backup(int j, int k, int d_i, int old_i, int i) {
//        auxListNode node = findAuxNode(k, backupArray[j].auxListArray[i]);
//        if (node !=null) {
//            fuseNode fuse = node.fuse;
//            fuse.updateNode(i, old_i, d_i);
//        } else {
//            int stackId = backupArray[j].tos[i]++;
//            if (stackId == backupArray[j].stack.size()) {
//                fuseNode fuse = new fuseNode(N, j);
//                backupArray[j].stack.add(fuse);
//            }
//            fuseNode p = backupArray[j].stack.get(stackId);
//            p.updateNode(i, old_i, d_i);
//            p.refCount++;
//            auxListNode a = new auxListNode();
//            a.fuse = p;
//            a.key = k;
//            p.auxNodesList[i] = a;
//            insertAuxNode(a, backupArray[j].auxListArray[i]);
//        }
//    }

//    public void crashAndRecover(int i) {
//        int[][] recover_sign = {
//                {1, 1, 0},
//                {1, 0, -1},
//                {0, 1, -1}
//        };
//        primListNode node = primaryArray[i].sPrimNode;
//        System.out.println("before crash, primary storage no." + i + " values: ");
//        node = node.next;
//        while (node != null) {
//         System.out.println("k :" + node.key + ", v: " + node.value);
//         node = node.next;
//        }
//
//        primaryArray[i].sPrimNode.next = null;
//        primaryArray[i].sAuxNode.next = null;
//        System.out.println("crashed, recovering...");
//
//        int size = backupArray[0].tos[i];
//        for (int q =0; q<size; q++) {
//            int k=0;
//            int val = 0;
//            for (int j=0; j<N; j++) {
//                fuseNode fuse = backupArray[j].stack.get(q);
//                val += recover_sign[i][j] * fuse.value;
//                k = fuse.auxNodesList[i].key;
//            }
//            val /= 2;
//            primListNode newNode = new primListNode();
//            newNode.key = k;
//            newNode.value = val;
//            auxListNode newAuxNode = new auxListNode();
//            newAuxNode.primNode = newNode;
//            newNode.auxNode = newAuxNode;
//            insertAuxNode(newAuxNode, primaryArray[i].sAuxNode);
//            insertPrimNode(newNode, primaryArray[i].sPrimNode);
//        }
//
//        System.out.println("recovered, primary storage no." + i + " values: ");
//        node = primaryArray[i].sPrimNode.next;
//        while (node != null) {
//            System.out.println("k :" + node.key + ", v: " + node.value);
//            node = node.next;
//        }
//    }