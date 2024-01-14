package org.zhu45.treetracker.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.zhu45.treetracker.common.Utils.appendCallerInfo;
import static org.zhu45.treetracker.common.logging.ConsoleColors.BLUE;
import static org.zhu45.treetracker.common.logging.ConsoleColors.BLUE_BOLD_BRIGHT;
import static org.zhu45.treetracker.common.logging.ConsoleColors.CYAN;
import static org.zhu45.treetracker.common.logging.ConsoleColors.GREEN;
import static org.zhu45.treetracker.common.logging.ConsoleColors.PURPLE;
import static org.zhu45.treetracker.common.logging.ConsoleColors.RED;
import static org.zhu45.treetracker.common.logging.ConsoleColors.RED_BOLD_BRIGHT;
import static org.zhu45.treetracker.common.logging.ConsoleColors.RESET;
import static org.zhu45.treetracker.common.logging.ConsoleColors.YELLOW;
import static org.zhu45.treetracker.common.logging.ConsoleColors.YELLOW_BACKGROUND;

public class LoggerProvider
{
    private LoggerProvider() {}

    public static class TreeTrackerLogger
    {
        private final Logger log;

        public TreeTrackerLogger(Class<?> clazz)
        {
            this.log = LogManager.getLogger(clazz);
        }

        public Boolean isDebugEnabled()
        {
            return log.isDebugEnabled();
        }

        public void debug(String s)
        {
            log.debug(appendCallerInfoLogger(s));
        }

        public void debug(String s, int stackTraceDepth)
        {
            log.debug(appendCallerInfo(s, stackTraceDepth));
        }

        public void debugRed(String s)
        {
            log.debug(appendCallerInfoLogger(RED + s + RESET));
        }

        public void debugRed(String s, int stackTraceDepth)
        {
            log.debug(appendCallerInfo(RED + s + RESET, stackTraceDepth));
        }

        public void debugGreen(String s)
        {
            log.debug(appendCallerInfoLogger(GREEN + s + RESET));
        }

        public void debugBlue(String s)
        {
            log.debug(appendCallerInfoLogger(BLUE + s + RESET));
        }

        public void debugCyan(String s)
        {
            log.debug(appendCallerInfoLogger(CYAN + s + RESET));
        }

        public void debugPurple(String s)
        {
            log.debug(appendCallerInfoLogger(PURPLE + s + RESET));
        }

        public void debugYellow(String s)
        {
            log.debug(appendCallerInfoLogger(YELLOW + s + RESET));
        }

        public void debugYellowBG(String s)
        {
            log.debug(appendCallerInfoLogger(YELLOW_BACKGROUND + s + RESET));
        }

        public void debugBlueBoldBright(String s)
        {
            log.debug(appendCallerInfoLogger(BLUE_BOLD_BRIGHT + s + RESET));
        }

        public void debugRedBoldBright(String s)
        {
            log.debug(appendCallerInfoLogger(RED_BOLD_BRIGHT + s + RESET));
        }

        public void info(String s)
        {
            log.info(appendCallerInfoLogger(s));
        }

        public void infoRed(String s)
        {
            log.info(appendCallerInfoLogger(RED + s + RESET));
        }

        public void infoBlue(String s)
        {
            log.info(appendCallerInfoLogger(BLUE + s + RESET));
        }

        public void infoYellowBG(String s)
        {
            log.info(appendCallerInfoLogger(YELLOW_BACKGROUND + s + RESET));
        }

        public void infoBlueBoldBright(String s)
        {
            log.info(appendCallerInfoLogger(BLUE_BOLD_BRIGHT + s + RESET));
        }

        public void warnRedBoldBright(String s)
        {
            log.warn(appendCallerInfoLogger(RED_BOLD_BRIGHT + s + RESET));
        }

        public void error(String s)
        {
            log.error(appendCallerInfoLogger(s));
        }

        public void error(Throwable exception, String message)
        {
            log.log(Level.FATAL, message, exception);
        }

        public void warn(String s)
        {
            log.warn(appendCallerInfoLogger(s));
        }

        private static String appendCallerInfoLogger(String s)
        {
            return appendCallerInfo(s, 3);
        }
    }

    public static TreeTrackerLogger getLogger(Class<?> clazz)
    {
        return new TreeTrackerLogger(clazz);
    }
}
