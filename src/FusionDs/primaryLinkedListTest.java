package FusionDs;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class primaryLinkedListTest {

    @Test
    void crashAndRecover() throws Exception {
        int n = 5, f = 3;
        fusionMatrix fm = new fusionMatrix(n, 3);
        primaryLinkedList[] lists = new primaryLinkedList[n];
        backupLinkedList[] b_lists = new backupLinkedList[f];
        for (int i = 0; i<n; i++) {
            lists[i] = new primaryLinkedList(fm.matrix, i);
        }
        for (int i = 0; i<f; i++) {
            b_lists[i] = new backupLinkedList(n, fm.matrix, n+i);
        }
        for (int i = 0; i<5; i++) {
            Random rand = new Random();
            for (int j = 0; j<1000; j++) {
                byte val = (byte) rand.nextInt(2);
                lists[i].insert(j, val);
                for (int k = 0; k<f; k++) {
                    b_lists[k].insert(i, j, 0, val);
                }
            }
        }
        int[] ids = {0, 1, 2, 5, 6};
        byte[][] vals = new byte[5][1000];
        for (int i=0; i<3; i++) {
            for (int j = 0; j<1000; j++) {
                vals[i][j] = lists[i].get(j).byteValue();
            }
        }
        for (int i=0; i<2; i++) {
            int[] res = b_lists[i].get_recovery();
            for (int j = 0; j<1000; j++) {
                vals[i+3][j] = (byte) res[j];
            }
        }
        lists[0].crashAndRecover(ids, vals);
    }
}