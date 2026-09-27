/*
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.core.util;

/** JVM and operating-system identity used by the host runtime. @opensearch.internal */
public final class Platform {
    private Platform() {}

    public static final String OS_NAME = System.getProperty("os.name", "");
    public static final String OS_ARCH = System.getProperty("os.arch", "");
    public static final String OS_VERSION = System.getProperty("os.version", "");
    public static final String JVM_NAME = System.getProperty("java.vm.name", "");
    public static final String JVM_VENDOR = System.getProperty("java.vm.vendor", "");
    public static final boolean LINUX = OS_NAME.startsWith("Linux");
    public static final boolean WINDOWS = OS_NAME.startsWith("Windows");
    public static final boolean MAC_OS_X = OS_NAME.startsWith("Mac OS X");
    public static final boolean FREE_BSD = OS_NAME.startsWith("FreeBSD");
    public static final boolean SUN_OS = OS_NAME.startsWith("SunOS");
}
