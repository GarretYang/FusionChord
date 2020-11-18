package Chord;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ChordRMI extends Remote {
    Response findSuccessor(Request r) throws RemoteException;

    Response findPredecessor(Request r) throws RemoteException;
//    Chord.Response findPredecessor(int id) throws RemoteException;
    Response notify(Request r) throws RemoteException;
    int getPort() throws RemoteException;

    void addNode(ChordNode node) throws RemoteException;

    Response getNID() throws RemoteException;

    Response getSuccessor() throws RemoteException;

    Response getPredecessor() throws RemoteException;

    Response findClosestPrecedingFinger(Request r) throws RemoteException;

    Response setPredecessor(Request r) throws RemoteException;

    Response setSuccessor(Request r) throws RemoteException;

    Response updateFingerTable(Request r) throws RemoteException;

    Response putKey(Request r) throws RemoteException;

    Response getKey(Request r) throws RemoteException;

    Response removeKey(Request r) throws RemoteException;

    Response migrateKey(Request r) throws RemoteException;
}
