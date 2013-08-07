package io.streamz;

public class KeyMaker {
    private final String keyPrefix;
    private final String hashKeyPrefix;
    private final String delimiter;

    public KeyMaker(String keyPrefix, String hashKeyPrefix, String delimiter) {
        this.keyPrefix = keyPrefix;
        this.hashKeyPrefix = hashKeyPrefix;
        this.delimiter = delimiter;
    }

    public String key(String key) {
        if (keyPrefix != null && !keyPrefix.isEmpty()) {
            return keyPrefix + delimiter + key;
        }
        else {
            return key;
        }
    }

    public String hkey(String key) {
        if (hashKeyPrefix != null && !hashKeyPrefix.isEmpty()) {
            return hashKeyPrefix + delimiter + key;
        }
        else {
            return key;
        }
    }
}
