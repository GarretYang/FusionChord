package Chord;

import java.io.Serializable;
import java.rmi.RemoteException;

/**
 * Please fill in the data structure you use to represent the request message for each RMI call.
 * Hint: Make it more generic such that you can use it for each RMI call.
 * Hint: Easier to make each variable public
 */
public class Request implements Serializable {
    static final long serialVersionUID=11L;
    // Your data here
    Integer ChordId;
    Integer key;
    Integer value;
    Integer fingerIndex;

    // Your constructor and methods here

    public Request(Integer ChordId) {
        this.ChordId = ChordId;
    }

    public Request(Integer ChordId, Integer key, Integer value) {
        this.ChordId = ChordId;
        this.key = key;
        this.value = value;
    }

    public Request(Integer ChordId, Integer fingerIndex) {
        this.ChordId = ChordId;
        this.fingerIndex = fingerIndex;
    }
}
