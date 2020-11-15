import org.junit.Test;
import static org.junit.Assert.*;

public class ChordTest {
    @Test
    public void TestInsertNode() {
        ChordNode root = new ChordNode(3,0);
        root.join(null);

        ChordNode nNode = new ChordNode(3, 2);
        nNode.join(root);

        for (int i = 0; i <= root.m; i++) {
            System.out.println(root.fingerTable[i]);
        }
    }
}
