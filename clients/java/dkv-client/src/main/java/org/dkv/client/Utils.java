package org.dkv.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static java.lang.String.format;

public class Utils {
    @SuppressWarnings({"unchecked", "ForwardCompatibility", "SpellCheckingInspection"})
    static void throwf(Class<? extends RuntimeException> excClass, String msg, Object... keys) {
        String errMsg = format(msg, keys);
        Constructor<?>[] cons = excClass.getConstructors();
        for (Constructor<?> con : cons) {
            Constructor<? extends RuntimeException> excClassCon = (Constructor<? extends RuntimeException>) con;
            Class<?>[] parameterTypes = excClassCon.getParameterTypes();
            if (parameterTypes.length == 1 && parameterTypes[0] == String.class) {
                try {
                    throw excClassCon.newInstance(errMsg);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException _) {
                    // Ignore, must never happen
                }
            }
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    static void checkf(boolean condition, Class<? extends RuntimeException> excClass, String msg, Object... keys) {
        if (!condition) {
            throwf(excClass, msg, keys);
        }
    }
}
