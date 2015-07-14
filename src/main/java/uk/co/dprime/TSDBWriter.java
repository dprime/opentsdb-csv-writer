package uk.co.dprime;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.log4j.Logger;


/**

 */
public class TSDBWriter {

    private static Cache<String, Number> lastSentCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(30, TimeUnit.MINUTES).build();
    public static String host;
    public static int port;


    private static ThreadLocal<ReliableChannel> channel = new ThreadLocal<ReliableChannel>() {
        @Override protected ReliableChannel initialValue() {
            return new ReliableChannel(host, port);
        }
    };

    private static final ExecutorService writerPool = Executors.newFixedThreadPool(20, new ThreadFactory() {
        private final AtomicInteger threadId = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "tsdbWriters worker " + threadId.incrementAndGet());
            return t;
        }
    });

    private static Logger log = Logger.getLogger(TSDBWriter.class);

    public static void write(String metric, Number value, long timestamp, Map<String, Object> tags) {
        writerPool.execute(() -> {
            String key =  metric + formatTags(tags);
            Number lastVal = lastSentCache.getIfPresent(key);
            if(lastVal != null && lastVal.equals(value)) {
                //sweet, skip it and move on.
            }
            else {
                //its either null or different, we need to write the data point and also update our last sent
                lastSentCache.put(key, value);
                String line =  "put " + metric + " " + timestamp + " " + value + " "+ formatTags(tags) + "\n";
                try {
                    channel.get().write(line);
                } catch (IOException e) {
                    try {
                        channel.get().connect();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    e.printStackTrace();
                }
            }
        });
    }


    private static String formatTags(Map<String, Object> tags) {
        if(tags == null || tags.isEmpty()) {
            return "";
        }
        else {
            final StringBuilder sb = new StringBuilder(10);

            tags.forEach((k, v) -> sb.append(k).append("=").append(v.toString()).append(" "));
            return sb.toString();
        }
    }



    private static class ReliableChannel {

        private SocketChannel channel;

        private final String hostName;
        private final int port;

        public ReliableChannel(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
        }


        public void write(String string) throws IOException {
            if (channel == null || !channel.isConnected()) {
                connect();
            }
            byte[] bytes = string.getBytes();
            ByteBuffer buf = ByteBuffer.allocate(bytes.length);
            buf.clear();
            buf.put(bytes);
            buf.flip();
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            read();
        }

        public void read() throws IOException {
            if (channel == null || !channel.isConnected()) {
                connect();
            }
            ByteBuffer buf = ByteBuffer.allocate(50);

            int bytesRead = channel.read(buf);
            while (bytesRead != 0) {

                buf.flip();

                String str;

                str = hostName + ": ";
                while (buf.hasRemaining()) {
                    str += (char) buf.get();
                }
                buf.clear();
                bytesRead = channel.read(buf);
            }

        }

        public void connect() throws IOException {
            if (channel != null) {
                channel.close(); //try and close it
            }
            channel = SocketChannel.open();
            channel.connect(new InetSocketAddress(hostName, port));
            channel.configureBlocking(false);
        }
    }


}