package paxos;

import java.io.Serializable;

/**
 * Created by meslahik on 15.11.17.
 */
public class Message implements Serializable{
    private int paxosInstanceId; // to distinguish the paxos instance this message belongs to
    private int waitingId; // used only by proposer
    private MessageType type;
    private int highestRound; // set only by acceptors; read by proposers
    private int round;
    private int value;
    private boolean hasValue = false;
    private int leaderId;
    private int senderId; // I used for distinguish diff acceptors in CATCHUP messages.
                          // Assume two diff learners send catchup; they receive two answers but both from one acceptor

    public Message() {

    }

    public Message(MessageType type, int value) {
        this.type = type;
        this.value = value;
    }

    public Message(MessageType type, int paxosInstanceId, int value) {
        this.type = type;
        this.paxosInstanceId = paxosInstanceId;
        this.value = value;
    }

    public Message(int paxosInstanceId, int waitingId, MessageType type, int round) {
        this.paxosInstanceId = paxosInstanceId;
        this.waitingId = waitingId;
        this.type = type;
        this.round = round;
    }

    public Message(int paxosInstanceId, int waitingId, MessageType type, int highestRound, int round, int value) {
        this.paxosInstanceId = paxosInstanceId;
        this.waitingId = waitingId;
        this.type = type;
        this.highestRound = highestRound;
        this.round = round;
        this.value = value;
    }

    void setValue(int value) {
        this.value = value;
    }
    void setHighestRound(int highestRound) { this.highestRound = highestRound; }
    void setPaxosInstanceId(int paxosInstanceId) { this.paxosInstanceId = paxosInstanceId; }
    void setType(MessageType type) { this.type = type; }
    void setLeaderId(int leaderId) { this.leaderId = leaderId; }
    void setSenderId(int senderId) { this.senderId = senderId; }
    void setHasValue(boolean hasValue) { this.hasValue = hasValue; }


    int getLeaderId() { return leaderId; }
    int getRound() { return round; }
    MessageType getType() { return type; }
    int getHighestRound() { return highestRound; }
    int getValue() { return value; }
    int getPaxosInstanceId() { return paxosInstanceId;}
    int getWaitingId() { return waitingId; }
    int getSenderId() { return senderId; }
    boolean getHasValue() { return hasValue; }

    @Override
    public String toString() {
        return "(paxosInstanceId=" + paxosInstanceId + ", waitingId=" + waitingId + ", type=" + type +
                ", hrnd=" + highestRound + ", rnd=" + round + ", val=" + value + ", senderId=" + senderId + ")";
    }

    void requestValue(int paxosInstanceId) {
        this.paxosInstanceId = paxosInstanceId;
    }
}
