package paxos;

import config.ConfigFileReader;
import ipmulticast.Receiver;
import ipmulticast.Sender;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;

/**
 * Created by meslahik on 15.11.17.
 */
public class Acceptor {
    // receive messages from proposers, return the result
    // one thread for rdeliver messages, put them in a list
    // main process do the job, poll for the list

    Logger logger;
    ConfigFileReader config;
    int acceptorId;

    Map<Integer, PaxosInstance> paxosInstances = new HashMap<>();

    // I suppose the main process polling the acceptorQueue would wait when it is empty, the thread can insert elements when it receives
    LinkedBlockingQueue<Message> proposerQueue = new LinkedBlockingQueue<>();
//    Queue<Message> acceptorQueue = new LinkedList<>();

    public Acceptor(int id, String configFile) {
        acceptorId = id;
        logger = LoggerFactory.getLogger(Acceptor.class);
        config = new ConfigFileReader(configFile);

        Thread reliableDeliverer = new Thread(new ReliableDeliverer());
        reliableDeliverer.start();
    }

    void sendToProposers(Message msg) {
        Sender sender = new Sender();
        sender.sendTo(msg, config.getProposersAddr().getIP(), config.getProposersAddr().getPort());
    }

    void sentToLearners(Message msg) {
        Sender sender = new Sender();
        sender.sendTo(msg, config.getLearnersAddr().getIP(), config.getLearnersAddr().getPort());
    }

    void execute() {
        while (true) {
            try {
                Message msg = proposerQueue.take();
                PaxosInstance instance = paxosInstances.get(msg.getPaxosInstanceId());

                logger.debug("processing msg {}", msg);
                switch (msg.getType()) {
                    case ONEA:
                        if (instance == null) {
                            logger.debug("create instance paxos {}", msg.getPaxosInstanceId());
                            instance = new PaxosInstance(msg.getPaxosInstanceId());
                            paxosInstances.put(instance.getPaxosInstanceId(), instance);
                        }
                        if (msg.getRound() > instance.getHighestRound()) {
                            logger.debug("message has a higher ballot={} than current hrnd={}. give the promise", msg.getRound(), instance.getHighestRound());
//                            instance.highestRound = msg.getRound();
                            instance.setHighestRound(msg.getRound(), msg.getSenderId());

                            Message newmsgONEB = new Message(instance.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
                                    instance.getHighestRound(), instance.getRound(), instance.getValue());
                            // if the acceptor has received a value before, set it has decided value
                            if (instance.hasValue)
                                newmsgONEB.setHasValue(true);
                            newmsgONEB.setSenderId(acceptorId);
                            logger.debug("send msg {} to proposers", newmsgONEB);
                            sendToProposers(newmsgONEB);
                        }
                        else if (msg.getRound() == instance.getHighestRound()) {
                            if (msg.getSenderId() == instance.getPropoesrId()) {
                                logger.debug("message with highest ballot from the same proposer {}. just reply the same as before", msg.getSenderId());
                                Message newmsgONEB = new Message(instance.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
                                        instance.getHighestRound(), instance.getRound(), instance.getValue());
                                newmsgONEB.setSenderId(acceptorId);
                                logger.debug("send msg {} to proposers", newmsgONEB);
                                sendToProposers(newmsgONEB);
                            } else
                                logger.error("It seems to different proposers are using the same proposal ids. The same highest round {} received form proposer {}. previously received from propoer {}",
                                        msg.getHighestRound(), msg.getSenderId(), instance.getPropoesrId());
                        } else {
                            logger.debug("message does not have higher ballot than current, notify the proposer");
                            Message newmsgONEB = new Message(instance.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
                                    instance.getHighestRound(), instance.getRound(), instance.getValue());
                            newmsgONEB.setSenderId(acceptorId);
                            logger.debug("send msg {} to proposers", newmsgONEB);
                            sendToProposers(newmsgONEB);
                        }

//                        if (instance == null) {
//                            PaxosInstance newInstance = new PaxosInstance(msg.getPaxosInstanceId());
//                            paxosInstances.put(newInstance.getPaxosInstanceId(), newInstance);
//                            newInstance.highestRound = msg.getRound();
//
//                            Message newmsg = new Message(newInstance.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
//                                    newInstance.getHighestRound(), newInstance.getRound(), newInstance.getValue());
//                            sendToProposers(newmsg);
//                        }
//                        else {
//                            instance.highestRound = msg.getRound(); //why not with synchronized()? because only one thread working on acceptorQueue. It may stop but the data won't be changed by another thread
//
//                            Message newmsg = new Message(msg.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
//                                    instance.getHighestRound(), instance.getRound(), instance.getValue());
//                            sendToProposers(newmsg);
//
////                            if (msg.getRound() >= instance.getHighestRound()) {
////                                instance.highestRound = msg.getRound(); //why not with synchronized()? because only one thread working on acceptorQueue. It may stop but the data won't be changed by another thread
////
////                                Message newmsg = new Message(msg.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
////                                        instance.getHighestRound(), instance.getRound(), instance.getValue());
////                                sendToProposers(newmsg);
////                            }
////                            else if (msg.getRound() < instance.getHighestRound()) { // don't need to answer
////                                instance.highestRound = msg.getRound();
////
////                                Message newmsg = new Message(msg.getPaxosInstanceId(), msg.getWaitingId(), MessageType.ONEB,
////                                        instance.getHighestRound(), instance.getRound(), instance.getValue());
////                                sendToProposers(msg);
////                            }
////                            else if (msgStored.type == MessageType.TWOA) {
////                                msg.round = msgStored.round;
////                                msg.value = msgStored.value;
////                                msg.type = MessageType.ONEB;
////                                sendToProposers(msg);
////                            }
//                        }
                        break;
                    case TWOA:
                        if (instance == null) { // when it has not received 1A before
                            logger.debug("does not contain instance but received TWOA message. create instance={} for that", msg.getPaxosInstanceId());
                            instance = new PaxosInstance(msg.getPaxosInstanceId());
                            paxosInstances.put(instance.getPaxosInstanceId(), instance);
                        }
                        if (msg.getRound() == instance.getHighestRound()) { // TODO: Is this correct?
                            logger.debug("message has bigger or equal ballot={} than current hrnd={}", msg.getRound(), instance.getHighestRound());
//                            instance.highestRound = msg.getRound();
                            instance.setHighestRound(msg.getRound(), msg.getSenderId());
                            instance.round = msg.getRound();
                            instance.value = msg.getValue();
                            instance.hasValue = true;
                        } else if (msg.getRound() < instance.getHighestRound()){
                            logger.debug("message has lower ballot than current, notify the proposer");
                            Message newmsgTWOB = new Message(instance.getPaxosInstanceId(), msg.getWaitingId(), MessageType.TWOB,
                                    instance.getHighestRound(), instance.getRound(), instance.getValue());
                            newmsgTWOB.setSenderId(acceptorId);
                            logger.debug("send msg {} to proposers", newmsgTWOB);
                            sendToProposers(newmsgTWOB);
                        }

                        Message newmsgTWOB = new Message(instance.getPaxosInstanceId(), msg.getWaitingId(), MessageType.TWOB,
                                instance.getHighestRound(), instance.getRound(), instance.getValue());
                        newmsgTWOB.setSenderId(acceptorId);
                        logger.debug("send msg {} to proposers", newmsgTWOB);
                        sendToProposers(newmsgTWOB);

//                        if (msg.round == msgStored.highestRound) {
//                            msgStored.round = msg.round;
//                            msgStored.value = msg.value;
//
//                            msg.type = MessageType.TWOB;
//                            sendToProposers(msg);
//                        }
//                        else {
//                            msg.highestRound = msgStored.highestRound;
//                            msg.round = msgStored.round;
//                            msg.value = msgStored.value;
//                            msg.type = MessageType.TWOB;
//                            sendToProposers(msg);
//                        }
                        break;
                    case CATCHUP:
                        if (instance != null && instance.hasValue) {
                            Message newmsg = new Message(MessageType.CATCHUP, instance.paxosInstanceId, instance.value);
                            newmsg.setSenderId(acceptorId);
                            logger.debug("value has been set. send msg {} to Learners", newmsg);
                            sentToLearners(newmsg);
                        }
                        else if (instance == null)
                            logger.debug("value has not been set. paxos instance {} does not exist", msg.getPaxosInstanceId());
                        else
                            logger.debug("value has not been set. paxos instance is {}", instance);
                }
            } catch (Exception ex) {
                System.out.println("Something went wrong reading the acceptorQueue of messages");
                ex.printStackTrace();
                break;
            }
        }
    }

    class PaxosInstance {
        private int paxosInstanceId;
        private int highestRound;
        private int round;
        private int value;
        private int propoesrId;
        private boolean hasValue;

        void setHighestRound(int round, int proposerId) {
            this.highestRound = round;
            this.propoesrId = proposerId;
        }

        public PaxosInstance(int paxosInstanceId) {
            this.paxosInstanceId = paxosInstanceId;
        }

        int getPaxosInstanceId() { return paxosInstanceId; }
        int getHighestRound() { return highestRound; }
        int getRound() { return round; }
        int getValue() { return value; }
        int getPropoesrId() { return propoesrId; }

        @Override
        public String toString() {
            return "(paxosInstance=" + paxosInstanceId + ", highestRound=" + highestRound + ", round=" + round + ", value=" + value + ")";
        }
    }

    class ReliableDeliverer implements Runnable{
        Receiver receiver;

        public ReliableDeliverer() {
            String host = config.getAcceptorsAddr().getIP();
            int port = config.getAcceptorsAddr().getPort();
            receiver = new Receiver(host, port);
        }

//        public boolean rdeliver(Message msg) {
//            acceptorQueue.add(msg);
//            return true;
//        }

        @Override
        public void run() {
            // What does it do? it waits for the messages and insert them in the acceptorQueue
            // How it waits?
            // How others find this class to send message to it?
            // maybe IP Multicast? subscribe and wait for the message?

            while(true) {
                try {
                    Message msg = (Message) receiver.receive();
                    logger.debug("received msg {}", msg);
                    proposerQueue.put(msg);
                } catch (InterruptedException ex) {
                    System.out.println("Error in putting in the queue");
                    ex.printStackTrace();
                }
            }
        }
    }

    static void Usage() {
        System.out.println("Usage: java Acceptor <id> <config-file>");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            Usage();
            return;
        }

        int id = Integer.parseInt(args[0]);
        String configFile = args[1];

        Acceptor acceptor = new Acceptor(id, configFile);
        acceptor.execute();
    }
}
