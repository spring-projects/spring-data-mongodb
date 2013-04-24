package org.springframework.data.mongodb.core.aggregation;

import org.springframework.util.Assert;

/**
 * Utility class for mongo db reference operator <code>$</code>
 */
public class ReferenceUtil {

    public static final String REFERENCE_PREFIX = "$";

    /**
     * ensures that the returned string begins with {@link #REFERENCE_PREFIX $}
     * @param key reference key with or without {@link #REFERENCE_PREFIX $} at the beginning
     * @return key that definitely begins with {@link #REFERENCE_PREFIX $}
     */
    public static String safeReference(String key) {

        Assert.hasText(key);

        if (!key.startsWith(REFERENCE_PREFIX)) {
            return REFERENCE_PREFIX + key;
        } else {
            return key;
        }
    }
}
