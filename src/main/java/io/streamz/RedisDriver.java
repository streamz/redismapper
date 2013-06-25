package io.streamz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RedisDriver extends Configured implements Tool {
    public static final String REDIS_HOST = "io.streamz.redismapper.redis.host";
    public static final String REDIS_PORT = "io.streamz.redismapper.redis.port";
    public static final String REDIS_KEY_FIELD = "io.streamz.redismapper.redis.key.field";
    public static final String REDIS_HASHKEY_FIELD = "io.streamz.redismapper.redis.hash.key.field";
    public static final String REDIS_HASHVAL_FIELD = "io.streamz.redismapper.redis.hash.val.field";
    public static final String REDIS_KEY_FILTER = "io.streamz.redismapper.redis.key.filter";
    public static final String REDIS_HASH_FILTER = "io.streamz.redismapper.redis.hash.filter";
    public static final String REDIS_VAL_FILTER = "io.streamz.redismapper.redis.val.filter";
    public static final String REDIS_KEY_TTL = "io.streamz.redismapper.redis.key.ttl";
    public static final String REDIS_KEY_TS = "io.streamz.redismapper.redis.key.ts";
    private static final String REDIS_CMD = "-redis";
    private static final String INPUT_CMD = "-input";
    private static final String KEY_CMD = "-key";
    private static final String HASH_KEY_CMD = "-hkey";
    private static final String HASH_VAL_CMD = "-hval";
    private static final String KEY_FILTER_CMD = "-kf";
    private static final String HASH_FILTER_CMD = "-hf";
    private static final String VAL_FILTER_CMD = "-vf";
    private static final String TTL_CMD = "-ttl";
    private static final String TS_KEY_CMD = "-tsk";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedisDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            usage();
            return 1;
        }

        Map<String, String> argMap = new HashMap<String, String>();
        String[] kv;

        for (String arg : args) {
            kv = arg.split("=");
            if (kv.length != 2) {
                usage();
                return 1;
            }
            argMap.put(kv[0].trim(), kv[1]);
        }

        Configuration conf = getConf();
        String[] hostPort = argMap.get(REDIS_CMD).split(":");
        conf.set(REDIS_HOST, hostPort[0].trim());
        conf.setInt(REDIS_PORT, Integer.valueOf(hostPort[1].trim()));
        conf.setInt(REDIS_KEY_FIELD, Integer.valueOf(argMap.get(KEY_CMD).trim()));
        conf.setInt(REDIS_HASHKEY_FIELD, Integer.valueOf(argMap.get(HASH_KEY_CMD).trim()));
        conf.setInt(REDIS_HASHVAL_FIELD, Integer.valueOf(argMap.get(HASH_VAL_CMD).trim()));

        if (argMap.containsKey(KEY_FILTER_CMD)) {
            conf.setPattern(REDIS_KEY_FILTER, Pattern.compile(argMap.get(KEY_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(HASH_FILTER_CMD)) {
            conf.setPattern(REDIS_HASH_FILTER, Pattern.compile(argMap.get(HASH_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(VAL_FILTER_CMD)) {
            conf.setPattern(REDIS_VAL_FILTER, Pattern.compile(argMap.get(VAL_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(VAL_FILTER_CMD)) {
            conf.setPattern(REDIS_VAL_FILTER, Pattern.compile(argMap.get(VAL_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(TTL_CMD)) {
            conf.setInt(REDIS_KEY_TTL, Integer.valueOf(argMap.get(TTL_CMD).trim()));
        }
        if (argMap.containsKey(TS_KEY_CMD)) {
            conf.set(REDIS_KEY_TS, argMap.get(TS_KEY_CMD).trim());
        }
        else {
            conf.set(REDIS_KEY_TS, "redis.lastupdate");
        }

        Job job = new Job(conf, "RedisDriver");
        FileInputFormat.addInputPath(job, new Path(argMap.get(INPUT_CMD)));
        job.setJarByClass(RedisDriver.class);
        job.setMapperClass(RedisOutputMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(RedisOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("" +
            "Usage: redisMapper " +
            REDIS_CMD + " <host:port> " +
            KEY_CMD + " <int> " +
            HASH_KEY_CMD + " <int> " +
            HASH_VAL_CMD + " <int> " +
            INPUT_CMD + " <path> " +
            TTL_CMD + " <ttl in seconds> " +
            TS_KEY_CMD + " <key of last update timestamp> ");
        ToolRunner.printGenericCommandUsage(System.err);
    }
}
