package org.pragmaticminds.crunch;

/**
 * Static class that holds logging information used for "trace" logging, i.e., logging of the raw event stream.
 *
 * @author julian
 * Created by julian on 10.09.18
 */
public class LoggingUtil {

    /**
     * After how many logs a trace output is printed
     */
    private static int TRACE_LOG_REPORT_CHECKPOINT = 1_000;

    private LoggingUtil() {
        // Do not instantiate
    }

    public static int getTraceLogReportCheckpoint() {
        return TRACE_LOG_REPORT_CHECKPOINT;
    }

    public static void setTraceLogReportCheckpoint(int traceLogReportCheckpoint) {
        TRACE_LOG_REPORT_CHECKPOINT = traceLogReportCheckpoint;
    }

}
