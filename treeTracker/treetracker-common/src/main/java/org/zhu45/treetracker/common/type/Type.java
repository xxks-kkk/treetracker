package org.zhu45.treetracker.common.type;

public interface Type
{
    /**
     * Returns the name of this type that should be displayed to end-users.
     */
    String getDisplayName();

    /**
     * Gets the Java class type used to represent this value on the stack during
     * expression execution.
     * <p>
     * Currently, this must be boolean, long, double, Slice or Block. (NOT TRUE NOW IN TREETRACKER!)
     */
    Class<?> getJavaType();

    /**
     * Gets the type length
     */
    int getLength();
}
