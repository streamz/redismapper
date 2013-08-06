package io.streamz;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RedisHMRecordWriter extends RecordWriter<Text, Text> {
    private JedisPool pool;
    private final String lastUpdateKey;
    private final int ttl;

    public RedisHMRecordWriter(String host, int port, String pw, int db, String lastUpdateKey, int ttl) {
        init(host, port, pw, db, 5);
        this.lastUpdateKey = lastUpdateKey;
        this.ttl = ttl;
    }

    @Override
    public void write(Text text, Text tuple)
        throws IOException, InterruptedException {
        String[] kv = tuple.toString().split(",");
        write(text.toString(), kv[0].trim(), kv[1].trim());
    }

    @Override
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
        Jedis connection = null;
        try {
            connection = pool.getResource();
            connection.set(lastUpdateKey, new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date()));
        }
        finally {
            pool.returnResource(connection);
            pool.destroy();
        }
    }

    private void write(String key, String hkey, String payload) {
        Jedis connection = null;
        try {
            connection = pool.getResource();
            connection.hset(key, hkey, payload);
            if (ttl > 0) {
                connection.expire(key, ttl);
            }
        }
        finally {
            pool.returnResource(connection);
        }
    }

    private boolean ping() {
        Jedis connection = null;
        try {
            connection = pool.getResource();
            return !connection.ping().isEmpty();
        }
        finally {
            pool.returnResource(connection);
        }
    }

    private boolean init(String host, int port, String pw, int db, long maxWait) {
        try {
            JedisPoolConfig conf = new JedisPoolConfig();
            conf.setMaxWait(maxWait);
            conf.setTestWhileIdle(true);
            conf.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);

            // 2 x the number of cores
            conf.setMaxActive(Runtime.getRuntime().availableProcessors() * 2);
            this.pool = new JedisPool(conf, host, port, Protocol.DEFAULT_TIMEOUT, pw, db);
            return ping();
        }
        catch (Exception ex) {
            throw new RuntimeException("Unable to connect to cache.", ex);
        }
    }
}
