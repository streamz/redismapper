package io.streamz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;
import java.util.regex.Pattern;

public class RedisOutputMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int keyIdx = conf.getInt(RedisDriver.REDIS_KEY_FIELD, -1);
        int hashIdx = conf.getInt(RedisDriver.REDIS_HASHKEY_FIELD, -1);
        int valIdx = conf.getInt(RedisDriver.REDIS_HASHVAL_FIELD, -1);

        if (keyIdx == -1 || hashIdx == -1 || valIdx == -1)
            return;

        String[] payload = StringUtils.getStrings(value.toString());
        String keyStr = payload[keyIdx];
        String hashStr = payload[hashIdx];
        String valStr = payload[valIdx];

        // check filters
        Pattern p = conf.getPattern(RedisDriver.REDIS_KEY_FILTER, null);
        if (p != null && p.matcher(keyStr).find()) {
            return;
        }

        p = conf.getPattern(RedisDriver.REDIS_HASH_FILTER, null);
        if (p != null && p.matcher(hashStr).find()) {
            return;
        }

        p = conf.getPattern(RedisDriver.REDIS_VAL_FILTER, null);
        if (p != null && p.matcher(valStr).find()) {
            return;
        }

        outkey.set(keyStr);
        outvalue.set(hashStr + "," + valStr);
        context.write(outkey, outvalue);
    }
}
