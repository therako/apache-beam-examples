package ApacheBeamExamples.Utils;

public class StackTraceUtil {
    public static String getFullStackTrace(Exception e, char delimiter) {
        StackTraceElement[] stack = e.getStackTrace();
        String theTrace = "";
        for(StackTraceElement line : stack)
        {
            theTrace += delimiter + line.toString();
        }
        return theTrace;
    }
}