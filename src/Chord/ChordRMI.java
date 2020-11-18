package Chord;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ChordRMI extends Remote {
    Response findSuccessor(int ChordId) throws RemoteException;
    Response findPredecessor(int ChordId) throws RemoteException;
//    Chord.Response findPredecessor(int id) throws RemoteException;
    Response notify(Request r) throws RemoteException;
    int getPort() throws RemoteException;

    void addNode(ChordNode node) throws RemoteException;

    Response getNID() throws RemoteException;

    Response getSuccessor() throws RemoteException;

    Response getPredecessor() throws RemoteException;

    Response findClosestPrecedingFinger(int ChordId) throws RemoteException;

    Response setPredecessor(int Predecessor) throws RemoteException;

    Response setSuccessor(int Successor) throws RemoteException;

    Response updateFingerTable(int ChordId, int fingerIndex) throws RemoteException;

    Response putKey(int key, int value) throws RemoteException;

    Response getKey(int key) throws RemoteException;
}
