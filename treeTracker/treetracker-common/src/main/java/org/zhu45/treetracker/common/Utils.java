package org.zhu45.treetracker.common;

import com.google.common.base.Joiner;

import java.util.Collection;

public class Utils
{
    private Utils() {}

    public static String properPrintList(Collection<?> list)
    {
        return Joiner.on(", ").join(list);
    }

    /**
     * This function compares inputs as collections. The key here is the ignore of the ordering
     *
     * @param expected one collection
     * @param actual the other collection
     * @param predicate the Predicate interface for custom comparator function
     * (passed in via either lambda or method reference)
     * @return true if collections have the same elements
     */
    public static boolean collectionCompare(Collection<?> actual, Collection<?> expected, Predicate predicate)
    {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (Object val1 : expected) {
            boolean foundEqual = false;
            for (Object val2 : actual) {
                if (predicate.predicate(val1, val2)) {
                    foundEqual = true;
                    break;
                }
            }
            if (!foundEqual) {
                return false;
            }
        }
        return true;
    }

    public static String appendCallerInfo(String s, int stackTraceDepth)
    {
        StackTraceElement e = Thread.currentThread().getStackTrace()[stackTraceDepth];
        return String.format("(%s|%s|%s): \n%s",
                e.getFileName(),
                e.getMethodName(),
                e.getLineNumber(),
                s);
    }

    public static String formatTraceMessageWithDepth(String message, int traceDepth)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < traceDepth; ++i) {
            stringBuilder.append("|");
        }
        stringBuilder.append(message);
        return stringBuilder.toString();
    }
}
