package monitor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by meslahik on 16.12.17.
 */
public class Monitor {
    Timer tpTimer = new Timer();
    Timer tpAvgTimer = new Timer();

    int tp;
    int prevSecondTp;
    int tpSecondIndex = 1;
    Map<Integer, Integer> tps = new ConcurrentHashMap<>();

    float latency;
    int latencyCount; // to compute each latency addition
    int latencySecondIndex = 1;
    Map<Integer, Float> latencies = new ConcurrentHashMap<>();

    public boolean isFinished = false;

    BufferedWriter tpWriter;
    BufferedWriter tpAvgWriter;
    BufferedWriter latencyWriter;
    BufferedWriter latencyAvgWriter;

    public Monitor(int duration, int warmUp) {
        initFiles(duration, warmUp);
        defineTimers(duration, warmUp);
    }

    void initFiles(int duration, int warmup) {
        try {
            tpWriter = new BufferedWriter(new FileWriter("../throughput.out"));
            tpWriter.write("# throughput - each line shows the tp in one second");
            tpWriter.flush();

            tpAvgWriter = new BufferedWriter(new FileWriter("../throughput-average.out"));
            tpAvgWriter.write("# average throughput - duration= " + duration + ", warmup= " + warmup);
            tpAvgWriter.flush();

            latencyWriter = new BufferedWriter(new FileWriter("../latency.out"));
            latencyWriter.write("# latency (ms) - each line shows the latency in one second");
            latencyWriter.flush();

            latencyAvgWriter = new BufferedWriter(new FileWriter("../latency-average.out"));
            latencyAvgWriter.write("# average latency (ms) - duration= " + duration + ", warmup= " + warmup);
            latencyAvgWriter.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    void defineTimers(int duration, int warmup) {
        // One-second Timers
        tpTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                // save the tp of one second
                // write to file

                // tp
                int diff = tp - prevSecondTp;
                tps.put(tpSecondIndex, diff);
                writeToFileTp(diff);
                prevSecondTp = tp;
                tpSecondIndex++;

                // latency
                latencies.put(latencySecondIndex, latency);
                writeToFileLatency(latency);
                latency = 0;
                latencyCount= 0;
                latencySecondIndex++;
            }
        },1000,1000);

        // Averages
        tpAvgTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                // compute average of duration minus warmup period
                // write to file

                int tpSum = 0;
                float latencySum = 0;
                for (int i=warmup+1; i <= duration; i++) {
                    tpSum += tps.get(i);
                    latencySum += latencies.get(i);
                }

                // tp
                int tpAvg = tpSum / (duration - warmup);
                writeToFileTpAvg(tpAvg);

                // latency
                float latencyAvg = latencySum / (duration - warmup);
                writeToFileLatencyAvg(latencyAvg);

                isFinished = true;
                tpTimer.cancel();
            }
        },(duration+2)*1000); // +5 to make sure that enough tp is written
    }

    void writeToFileTp(int tp) {
        try {
            tpWriter.append("\n" + tp);
            tpWriter.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    void writeToFileLatency(float latency) {
        try {
            latencyWriter.append("\n" + latency);
            latencyWriter.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    void writeToFileTpAvg(int tpAvg) {
        try {
            tpAvgWriter.append("\n" + tpAvg);
            tpAvgWriter.flush();
        } catch (IOException ex) {

        }
    }

    void writeToFileLatencyAvg(float latencyAvg) {
        try {
            latencyAvgWriter.append("\n" + latencyAvg);
            latencyAvgWriter.flush();
        } catch (IOException ex) {

        }
    }

    public void incrementTp() {
        tp++;
    }

    // in milliseconds
    public void addLatency(long latency) {
        this.latency = this.latency*latencyCount + latency;
        latencyCount++;
        this.latency = this.latency / latencyCount;
    }
}
