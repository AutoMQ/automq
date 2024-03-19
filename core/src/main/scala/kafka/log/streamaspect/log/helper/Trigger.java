package kafka.log.streamaspect.log.helper;



/**
 * @author ipsum-0320
 */
public class Trigger {
    // 使用 Trigger 类需要严格保证线程安全。
    private static Boolean isScan = false;

    public static void setScan(boolean isScan) {
        Trigger.isScan = isScan;
    }

    public static boolean getScan() {
        return Trigger.isScan;
    }
}
