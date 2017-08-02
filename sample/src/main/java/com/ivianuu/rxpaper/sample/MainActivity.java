package com.ivianuu.rxpaper.sample;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.util.Pair;

import com.ivianuu.rxpaper.RxBook;
import com.ivianuu.rxpaper.RxPaper;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.reactivex.CompletableSource;
import io.reactivex.Observable;
import io.reactivex.SingleSource;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

public class MainActivity extends AppCompatActivity {

    private int count;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        RxPaper.init(this);

        final RxBook rxBook = RxPaper.book();
    }
}
