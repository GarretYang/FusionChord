package Chord;

import java.io.Serializable;
import java.util.Map;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=22L;
    // your data here
    Object value;
    int ChordId;
    Map<Integer, Integer> hm;
    // Your constructor and methods here
    public Response(Object value) {
        this.value = value;
    }

    public Response(Object value, int ChordId) {
        this.ChordId = ChordId;
        this.value = value;
    }

    public Response(Map<Integer, Integer> hm) {
        this.hm = hm;
    }
}
