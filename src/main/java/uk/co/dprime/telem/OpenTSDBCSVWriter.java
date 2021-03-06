package uk.co.dprime.telem;

import uk.co.dprime.TSDBWriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OpenTSDBCSVWriter {

    private final String inputfile;

    public static void main(String[] args) throws IOException, InterruptedException {
        String inputfile = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        OpenTSDBCSVWriter spammer = new OpenTSDBCSVWriter(inputfile, host, port);
        while(true) {
            long now = System.currentTimeMillis();
            spammer.start(now);
        }
    }

    public OpenTSDBCSVWriter(String inputfile, String host, int port) {
        this.inputfile = inputfile;
        //because the writer is static, set the host/port values here so that when the threadlocal kicks
        //in it connects to the right place.
        TSDBWriter.host = host;
        TSDBWriter.port = port;
    }


    public void start(long baseTime) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new FileReader(inputfile));
        String line;
        String[] headers = null;
        Map<String, Object> tags = new HashMap<>();
        tags.put("tag", "value");

        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");

            if(headers == null) {
                headers = split;
                continue;
            }

            long now = System.currentTimeMillis();

            String time = split[0];

            long usSinceStartx10 = Long.valueOf(time); //interval currently in units of 10uS, need to turn it into real millis, so /100

            //how long since start in millis?
            long millisSinceStart = usSinceStartx10/100;

            //what's the difference between now-startTime and millisSinceStart
            long millisSinceBase = now - baseTime;
            long diff = millisSinceStart - millisSinceBase;

            //otherwise, do the actual sending
            for (int i = 1; i < split.length; i++) {
                String bit = split[i];
                if(bit == null || bit.length() == 0) {
                    continue;
                }
                String header = headers[i];
                header = header.replaceAll("_", ".").replaceAll(" ", ".");
                String metric = header;

                TSDBWriter.write(metric, Double.parseDouble(bit), baseTime+millisSinceStart, tags);
            }

            System.out.println("put " + diff);
            if(diff > 0) {
                Thread.sleep(diff);
            }
        }
    }
}
