package com.ivianuu.rxpaper;

import android.content.Context;

import com.esotericsoftware.kryo.Serializer;

import io.paperdb.Paper;
import io.reactivex.Scheduler;
import io.reactivex.annotations.NonNull;
import io.reactivex.schedulers.Schedulers;

/**
 * Wraps the paper class
 */
public final class RxPaper {

    private RxPaper() {
        // no instances
    }

    /**
     * Must be used before using the library
     */
    public static void init(@NonNull Context context) {
        Paper.init(context.getApplicationContext());
    }

    /**
     * Adds the serializer
     */
    public static <T> void addSerializer(@NonNull Class<T> clazz, @NonNull Serializer<T> serializer) {
        Paper.addSerializer(clazz, serializer);
    }

    /**
     * Sets the log level
     */
    public static void setLogLevel(int level) {
        Paper.setLogLevel(level);
    }

    /**
     * Returns the default book
     */
    public static RxBook book() {
        return book(Schedulers.io());
    }

    /**
     * Returns the default book
     */
    public static RxBook book(@NonNull Scheduler scheduler) {
        return new RxBook(Paper.book(), scheduler);
    }

    /**
     * Returns a custom book
     */
    public static RxBook customBook(@NonNull String name) {
        return customBook(name, Schedulers.io());
    }

    /**
     * Returns a custom book
     */
    public static RxBook customBook(@NonNull String name, @NonNull Scheduler scheduler) {
        return new RxBook(Paper.book(name), scheduler);
    }
}
