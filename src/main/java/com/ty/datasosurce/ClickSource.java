package com.ty.datasosurce;

import com.ty.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author tangyun
 * @date 2022/4/4 5:26 下午
 */
public class ClickSource implements SourceFunction<Event> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();

        String[] users = {"Alice", "Bob", "Cary", "Mary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=100", "./prod?id=200"};

        while (flag) {
            sourceContext.collect(
                    new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Instant.now().toEpochMilli())
            );
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
