package paxos;

import config.ConfigFileReader;
import ipmulticast.Receiver;
import ipmulticast.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by meslahik on 15.11.17.
 */
public class Learner {
    Logger logger;
    private int learnerId;
    ConfigFileReader config;
    Map<Integer, Integer> values = new HashMap<>();
    int largestLearntId = 0;
    Map<Integer, Message> waitingValues = new HashMap<>();
    LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue();
    Map<Integer, PaxosInstance> requestValues = new ConcurrentHashMap<>();
    final int NUM_OF_ACCEPTORS = 3;
    final int MAJORITY = NUM_OF_ACCEPTORS/2 + 1;

    public Learner(int id, String configFile, boolean isCatchupEnabled) {
        logger = LoggerFactory.getLogger(Learner.class);
        learnerId = id;
        config = new ConfigFileReader(configFile);

        if (!isCatchupEnabled)
            receiveFirstMsg();

        Timer catchupTimer = new Timer();
        catchupTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (!requestValues.isEmpty()) {
                    requestValueHelper(Collections.min(requestValues.keySet()));
                }
            }
        }, 200, 20);

        Thread reliableDeliverer = new Thread(new ReliableDeliverer());
        reliableDeliverer.start();
    }

    void receiveFirstMsg() {
        logger.debug("catchup is disabled. check the instance id to start learning from");
        String host = config.getLearnersAddr().getIP();
        int port = config.getLearnersAddr().getPort();
        Receiver receiver = new Receiver(host, port);
        Message msg = (Message) receiver.receive();
        queue.add(msg);
        largestLearntId = msg.getPaxosInstanceId()-1;
        logger.debug("start learning value from instance {} with value {}", msg.getPaxosInstanceId(), msg.getValue());
    }

    void hasAdded(int value) { }

    boolean addValue(int paxosInstanceId, int value) {
        if (!values.containsKey(paxosInstanceId)) {
            values.put(paxosInstanceId, value);
            largestLearntId = paxosInstanceId;
            logger.debug("paxos instance={}, val={} learnt", paxosInstanceId, value);
            System.out.println(value);
            System.out.flush();
            hasAdded(value);

            if (requestValues.containsKey(paxosInstanceId)) {
                PaxosInstance instance = requestValues.get(paxosInstanceId);
//                instance.cancelTimer();
                requestValues.remove(paxosInstanceId);
                logger.debug("remove instance id {} from request set", paxosInstanceId);
            }
            return true;
        }
        return false;
    }

    void requestValueHelper(int paxosInstanceId) {
        Sender sender = new Sender();
        Message msg = new Message();
        msg.setType(MessageType.CATCHUP);
        msg.setPaxosInstanceId(paxosInstanceId);
        sender.sendTo(msg, config.getAcceptorsAddr().getIP(), config.getAcceptorsAddr().getPort());
        logger.debug("CATCHUP request sent to receive instance {} value", paxosInstanceId);
    }

//    void requestValue(int start, int end) {
//        for (int paxosInstanceId = start; paxosInstanceId < end; paxosInstanceId++) {
//            if (!values.containsKey(paxosInstanceId) && !waitingValues.containsKey(paxosInstanceId) && !requestValues.containsKey(paxosInstanceId)) { // may have learnt but waits for lower instances
//                PaxosInstance instance = new PaxosInstance(paxosInstanceId);
//                requestValueHelper(paxosInstanceId);
//                requestValues.put(instance.paxosInstanceId, instance);
//            } else if (values.containsKey(paxosInstanceId))
//                logger.debug("values list contains the paxos id {}. Ignore create paxos instance to CATCHUP", paxosInstanceId);
//            else if (waitingValues.containsKey(paxosInstanceId))
//                logger.debug("waiting list contains the paxos id {}. Ignore create paxos instance to CATCHUP", paxosInstanceId);
//            else if (requestValues.containsKey(paxosInstanceId))
//                logger.debug("request list contains the paxos id {}. Ignore create paxos instance to CATCHUP", paxosInstanceId);
//        }
//    }

    void requestValue(int paxosInstanceId) {
        if (!requestValues.containsKey(paxosInstanceId) && !waitingValues.containsKey(paxosInstanceId)) {
            PaxosInstance instance = new PaxosInstance(paxosInstanceId);
//            requestValueHelper(paxosInstanceId);
            requestValues.put(instance.paxosInstanceId, instance);
//            int i = Collections.min(requestValues.keySet());
//            requestValues.get(i);
        }
//        if (!values.containsKey(paxosInstanceId) && !waitingValues.containsKey(paxosInstanceId) && !requestValues.containsKey(paxosInstanceId)) { // may have learnt but waits for lower instances
//                PaxosInstance instance = new PaxosInstance(paxosInstanceId);
//                requestValueHelper(paxosInstanceId);
//                requestValues.put(instance.paxosInstanceId, instance);
//            } else if (values.containsKey(paxosInstanceId))
//                logger.debug("values list contains the paxos id {}. Ignore create paxos instance to CATCHUP", paxosInstanceId);
//            else if (waitingValues.containsKey(paxosInstanceId))
//                logger.debug("waiting list contains the paxos id {}. Ignore create paxos instance to CATCHUP", paxosInstanceId);
//            else if (requestValues.containsKey(paxosInstanceId))
//                logger.debug("request list contains the paxos id {}. Ignore create paxos instance to CATCHUP", paxosInstanceId);
    }

    void execute() {
        while(true) {
            Message msgW = waitingValues.get(largestLearntId + 1);
            if (msgW != null) {
                logger.debug("processing message... found paxos instance {} in waiting list.", msgW.getPaxosInstanceId());
                addValue(msgW.getPaxosInstanceId(), msgW.getValue());
            }
            else {
                try {
                    Message msgQ = queue.take();
                    logger.debug("processing message {}", msgQ);
                    if (msgQ.getType() == MessageType.THREE) {
                        if (msgQ.getPaxosInstanceId() == largestLearntId + 1) {
                            addValue(msgQ.getPaxosInstanceId(), msgQ.getValue());
                        } else if (msgQ.getPaxosInstanceId() > largestLearntId + 1) {
                            logger.debug("instance id {} received, largestLearntId is {}. try to ask from acceptors", msgQ.getPaxosInstanceId(), largestLearntId);
                            waitingValues.put(msgQ.getPaxosInstanceId(), msgQ);
                            for (int i= largestLearntId+1; i < msgQ.getPaxosInstanceId(); i++)
                                requestValue(i);
                        } else
                            logger.debug("ignore msg {}", msgQ);
                    }
                    else if (msgQ.getType() == MessageType.CATCHUP) {
                        if (values.containsKey(msgQ.getPaxosInstanceId()) || waitingValues.containsKey(msgQ.getPaxosInstanceId())) {
                            logger.debug("paxos id {} is learnt or waiting. ignore it", msgQ.getPaxosInstanceId());
                            continue;
                        }
                        PaxosInstance instance = requestValues.get(msgQ.getPaxosInstanceId()); // if this learner has not requested, ignore it
                        if (instance == null) {
                            logger.debug("this learder has not requested paxos id {}. ignore it", msgQ.getPaxosInstanceId());
                            continue;
                        }

                        boolean isAdded = instance.addAcceptorMessage(msgQ);
                        if (!isAdded)
                            continue;
                        if (!instance.hasMajorityReached())
                            continue;

                        logger.debug("Majority of CATCHUP messages received for paxos instance {}", instance.paxosInstanceId);

                        if (msgQ.getPaxosInstanceId() == largestLearntId + 1) {
                            addValue(msgQ.getPaxosInstanceId(), msgQ.getValue());

                        } else if (msgQ.getPaxosInstanceId() > largestLearntId + 1) {
                            logger.debug("instance id {} received, largestLearntId is {}. try to ask from acceptors", msgQ.getPaxosInstanceId(), largestLearntId);
                            waitingValues.put(msgQ.getPaxosInstanceId(), msgQ);
                            requestValue(largestLearntId+1);
                        } else
                            logger.debug("ignore msg {}", msgQ);
                    }
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    class PaxosInstance { // used for learning CATCHUP messages
        private int paxosInstanceId;
        private boolean isWaiting;
//        int numOfResponses = 0;
        Set<Integer> acceptorsId = new HashSet<>();
        Map<Integer, Integer> receivedValues = new HashMap<>();

        public PaxosInstance(int paxosInstanceId) {
            this.paxosInstanceId = paxosInstanceId;
            isWaiting = true;
        }


        boolean hasMajorityReached() {
            return !isWaiting;
        }

        boolean addAcceptorMessage(Message msg) { // TODO: what if two acceptors has different values??
            if (!isWaiting) {
                logger.debug("ignore msg {}. this instance is not waiting for any response", msg);
                return false;
            }
            acceptorsId.add(msg.getSenderId());
            logger.debug("for paxos id {} CATCHUP, received acceptor id {} response. acceptors responded: {}", msg.getPaxosInstanceId(), msg.getSenderId(), acceptorsId);

            if (receivedValues.containsKey(msg.getValue()))
                receivedValues.put(msg.getValue(), receivedValues.get(msg.getValue()) + 1);
            else
                receivedValues.put(msg.getValue(), 1);
            logger.debug("value {} with count={} in received values", msg.getValue(), receivedValues.get(msg.getValue()));
            boolean majority = receivedValues.keySet().stream().anyMatch(x -> receivedValues.get(x) == MAJORITY);
            if (majority)
                isWaiting = false;
            return true;
        }
    }

    class handleCatchupMessages implements Runnable {
        @Override
        public void run() {

        }
    }

    class ReliableDeliverer implements Runnable {
        Receiver receiver;

        public ReliableDeliverer() {
            String host = config.getLearnersAddr().getIP();
            int port = config.getLearnersAddr().getPort();
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
                Message msg = (Message) receiver.receive();
                logger.debug("received msg {}", msg);
                if (msg.getPaxosInstanceId() > largestLearntId) { // should never learn value with lower or equal id than previous learnt ids
                    queue.add(msg);
//                    logger.debug("msg {} added to the queue", msg);
                }
                else
                    logger.debug("msg {} ignored. msg.paxosInstanceId={}, largestLearntId={}", msg, msg.getPaxosInstanceId(), largestLearntId);
            }
        }
    }

    static void Usage() {
        System.out.println("Usage: java Learner <id> <config-file>");
    }

    public static void main(String[] args) {
        boolean IS_CATCHUP_ENABLED = true;

        if (args.length != 2) {
            Usage();
            return;
        }

        int id = Integer.parseInt(args[0]);
        String configFile = args[1];

        Learner learner = new Learner(id, configFile, IS_CATCHUP_ENABLED);
        learner.execute();
    }
}
