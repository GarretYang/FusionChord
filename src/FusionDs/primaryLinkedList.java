package FusionDs;

public class primaryLinkedList {
    Primary primary;
    Matrix matrix;
    int i;

    public primaryLinkedList(Matrix matrix, int i) {
        primary = new Primary();
        this.matrix = matrix;
        this.i = i;
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

    public void crashAndRecover(int[] ids, byte[][] vals) {
        int n = matrix.getColumns();
        assert ids.length == n;
        assert vals.length == n;

        byte[][] b_prime = new byte[n][n]; // n * n : B'
        for (int i = 0; i<n; i++) {
            for (int j = 0; j<n; j++) {
                b_prime[i][j] = matrix.get(ids[i], j);
            }
        }
        Matrix b_prime_mat = new Matrix(b_prime);

        int k = vals[0].length;
        for (int i=0; i<n; i++) {
            assert vals[i].length == k;
        }

        Matrix p_prime_mat = new Matrix(vals);
        Matrix res = b_prime_mat.invert().times(p_prime_mat);
        primListNode start = primary.sPrimNode.next;
        int j = 0;
        while (start != null) {
//            System.out.println(String.format("value: %d, new: %d", start.value, res.get(i,j)));
//            assert (start.value - res.get(i,j)) == 0;
            start = start.next;
            j++;
        }
    }

    private class Primary {
        primListNode sPrimNode;
        auxListNode sAuxNode;
        public Primary() {
            sPrimNode = new primListNode();
            sAuxNode = new auxListNode();
        }
    }

    private class primListNode {
        int key;
        int value;
        auxListNode auxNode;
        primListNode next;
        public primListNode(){
            auxNode = null;
            next = null;
        }
    }

    private class auxListNode {
        primListNode primNode;
        auxListNode next;
        int key;
        public auxListNode(){
            next = null;
            primNode = null;
        }
    }
}