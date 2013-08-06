package io.streamz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class RedisOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        String host = context.getConfiguration().get(RedisDriver.REDIS_HOST);
        int port = context.getConfiguration().getInt(RedisDriver.REDIS_PORT, 6379);
        String updateKey = context.getConfiguration().get(RedisDriver.REDIS_KEY_TS);
        String password = context.getConfiguration().get(RedisDriver.REDIS_PW, null);
        int ttl = context.getConfiguration().getInt(RedisDriver.REDIS_KEY_TTL, 0);
        int db = context.getConfiguration().getInt(RedisDriver.REDIS_DB, 0);
        return new RedisHMRecordWriter(host, port, password, db, updateKey, ttl);
    }

    @Override
    public void checkOutputSpecs(JobContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String host = conf.get(RedisDriver.REDIS_HOST);
        int key = conf.getInt(RedisDriver.REDIS_KEY_FIELD, -1);
        int hash = conf.getInt(RedisDriver.REDIS_HASHKEY_FIELD, -1);
        int val = conf.getInt(RedisDriver.REDIS_HASHVAL_FIELD, -1);
        if (host == null || host.isEmpty() || key == -1 || hash == -1 || val == -1)
            throw new IOException("Missing configuration param, check usage.");
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
    }
}
