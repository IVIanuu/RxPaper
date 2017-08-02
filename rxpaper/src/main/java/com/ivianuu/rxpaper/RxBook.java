package com.ivianuu.rxpaper;

import android.util.Pair;

import java.util.List;
import java.util.concurrent.Callable;

import io.paperdb.Book;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.MaybeEmitter;
import io.reactivex.MaybeOnSubscribe;
import io.reactivex.MaybeSource;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.processors.PublishProcessor;

/**
 * Wraps a book
 */
public final class RxBook {

    private final Book book;
    private final Scheduler scheduler;

    private final PublishProcessor<String> keyChangesProcessor = PublishProcessor.create();

    RxBook(@NonNull Book book, @NonNull Scheduler scheduler) {
        this.book = book;
        this.scheduler = scheduler;
    }

    /**
     * Destroys this book
     */
    @NonNull
    public Completable destroy() {
        return getAllKeys()
                .doOnSuccess(new Consumer<List<String>>() {
                    @Override
                    public void accept(List<String> keys) throws Exception {
                        book.destroy();
                        for (String key : keys) {
                            keyChangesProcessor.onNext(key);
                        }
                    }
                })
                .toCompletable()
                .subscribeOn(scheduler);
    }

    /**
     * Writes the value
     */
    @NonNull
    public <T> Completable write(@NonNull final String key, @NonNull final T value) {
        return Completable
                .fromCallable(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        book.write(key, value);
                        return new Object();
                    }
                })
                .doOnComplete(new Action() {
                    @Override
                    public void run() throws Exception {
                        keyChangesProcessor.onNext(key);
                    }
                })
                .subscribeOn(scheduler);
    }

    /**
     * Reads the value
     */
    @NonNull
    public <T> Maybe<T> read(@NonNull final String key) {
        return Maybe.create(new MaybeOnSubscribe<T>() {
            @Override
            public void subscribe(MaybeEmitter<T> e) throws Exception {
                T value = book.read(key);
                if (value != null) {
                    if (!e.isDisposed()) {
                        e.onSuccess(value);
                    }
                }

                if (!e.isDisposed()) {
                    e.onComplete();
                }
             }
        }).subscribeOn(scheduler);
    }

    /**
     * Reads the value
     */
    @NonNull
    public <T> Single<T> read(@NonNull final String key, @NonNull final T defaultValue) {
        return Single.fromCallable(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return book.read(key, defaultValue);
            }
        }).subscribeOn(scheduler);
    }

    /**
     * Returns if the key exists
     */
    @NonNull
    public Single<Boolean> exist(@NonNull final String key) {
        return Single.fromCallable(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return book.exist(key);
            }
        }).subscribeOn(scheduler);
    }

    /**
     * Returns the last modified time of the key
     */
    @NonNull
    public Single<Long> lastModified(@NonNull final String key) {
        return Single.fromCallable(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return book.lastModified(key);
            }
        }).subscribeOn(scheduler);
    }

    /**
     * Deletes the key
     */
    @NonNull
    public Completable delete(@NonNull final String key) {
        return Completable
                .fromCallable(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        book.delete(key);
                        return new Object();
                    }
                })
                .doOnComplete(new Action() {
                    @Override
                    public void run() throws Exception {
                        keyChangesProcessor.onNext(key);
                    }
                })
                .subscribeOn(scheduler);
    }

    /**
     * Returns all keys of this book
     */
    @NonNull
    public Single<List<String>> getAllKeys() {
        return Single.fromCallable(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return book.getAllKeys();
            }
        }).subscribeOn(scheduler);
    }

    /**
     * Returns all values
     */
    @NonNull
    public Single<List<Pair<String, Object>>> getAllValues() {
        return getAllValues(Object.class);
    }

    /**
     * Returns all the values of the given class
     */
    @NonNull
    public <T> Single<List<Pair<String, T>>> getAllValues(@NonNull final Class<T> clazz) {
        return getAllKeys()
                .toObservable()
                .flatMapIterable(new Function<List<String>, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(List<String> keys) throws Exception {
                        return keys;
                    }
                })
                .flatMapSingle(new Function<String, SingleSource<Pair<String, Object>>>() {
                    @Override
                    public SingleSource<Pair<String, Object>> apply(final String key) throws Exception {
                        return read(key, new Object())
                                .map(new Function<Object, Pair<String, Object>>() {
                                    @Override
                                    public Pair<String, Object> apply(Object value) throws Exception {
                                        return new Pair<>(key, value);
                                    }
                                });
                    }
                })
                .filter(new Predicate<Pair<String, Object>>() {
                    @Override
                    public boolean test(Pair<String, Object> pair) throws Exception {
                        return clazz.isInstance(pair.second);
                    }
                })
                .map(new Function<Pair<String, Object>, Pair<String, T>>() {
                    @Override
                    public Pair<String, T> apply(Pair<String, Object> pair) throws Exception {
                        return new Pair<>(pair.first, clazz.cast(pair.second));
                    }
                })
                .toList();
    }

    /**
     * Emits on key changes
     */
    @NonNull
    public Flowable<String> keyChanges() {
        return keyChangesProcessor.share();
    }

    /**
     * Emits on value changes
     * Emits on subscribe
     */
    @NonNull
    public <T> Flowable<T> latest(@NonNull final String key) {
        return keyChanges()
                .filter(new Predicate<String>() {
                    @Override
                    public boolean test(String changedKey) throws Exception {
                        return changedKey.equals(key);
                    }
                })
                .startWith("<init>") // Dummy value to trigger initial load.
                .flatMapMaybe(new Function<String, MaybeSource<? extends T>>() {
                    @Override
                    public MaybeSource<? extends T> apply(String s) throws Exception {
                        return read(key);
                    }
                });
    }

    /**
     * Emits on key changes
     * You have to cast the objects
     */
    @NonNull
    public Flowable<Pair<String, Object>> updates() {
        return updates(Object.class);
    }

    /**
     * Emits on key changes of the given class
     */
    @NonNull
    public <T> Flowable<Pair<String, T>> updates(final Class<T> clazz) {
        return keyChanges()
                .flatMapMaybe(new Function<String, MaybeSource<? extends Pair<String, ?>>>() {
                    @Override
                    public MaybeSource<? extends Pair<String, ?>> apply(final String key) throws Exception {
                        return read(key)
                                .map(new Function<Object, Pair<String,?>>() {
                                    @Override
                                    public Pair<String, ?> apply(Object value) throws Exception {
                                        return new Pair<>(key, value);
                                    }
                                });
                    }
                })
                .filter(new Predicate<Pair<String, ?>>() {
                    @Override
                    public boolean test(Pair<String, ?> pair) throws Exception {
                        return clazz.isInstance(pair.second);
                    }
                })
                .map(new Function<Pair<String, ?>, Pair<String, T>>() {
                    @Override
                    public Pair<String, T> apply(Pair<String, ?> pair) throws Exception {
                        return new Pair<>(pair.first, clazz.cast(pair.second));
                    }
                });
    }
}
