import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TimerSpin {
    public static void main(String[] args) throws Exception {
        var loop = new NioEventLoopGroup(1).next();
        long[] tid = new long[1];
        Runnable[] timer = new Runnable[1];
        // a timer every 1 ms, like io.vertx.core.Vertx#setTimer(1, ...)
        timer[0] = () -> { tid[0] = Thread.currentThread().threadId(); loop.schedule(timer[0], 1, TimeUnit.MILLISECONDS); };
        loop.schedule(timer[0], 1, TimeUnit.MILLISECONDS);
        // other threads wake the loop up now and then, like I/O events: 4000 tasks/s
        Thread t = new Thread(() -> {
            for (;;) {
                LockSupport.parkNanos((long) (-Math.log(1 - Math.random()) * 250_000));
                loop.execute(() -> { });
            }
        });
        t.setDaemon(true);
        t.start();
        TimeUnit.SECONDS.sleep(1);
        var mx = ManagementFactory.getThreadMXBean();
        long before = mx.getThreadCpuTime(tid[0]);
        TimeUnit.SECONDS.sleep(5);
        long ms = (mx.getThreadCpuTime(tid[0]) - before) / 1_000_000;
        System.out.println(io.netty.util.Version.identify().get("netty-transport").artifactVersion()
                + ": event loop CPU " + ms / 50.0 + "% of a core");
        System.exit(0);
    }
}
