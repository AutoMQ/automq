package kafka.log.es;

import java.util.concurrent.CompletableFuture;

public class FutureUtil {
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        cf.completeExceptionally(ex);
                return cf;
    }
}
