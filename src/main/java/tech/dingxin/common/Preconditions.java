package tech.dingxin.common;

import javax.annotation.Nullable;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Preconditions {
    public static <T> T checkNotNull(@Nullable T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }
}
