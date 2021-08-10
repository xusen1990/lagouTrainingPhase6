package com.lagou.demo;

import java.util.concurrent.ArrayBlockingQueue;

public class App {

    public static void main(String[] args) {
        ArrayBlockingQueue<KouZhao> queue = new ArrayBlockingQueue<>(20);

        Producer producer = new Producer(queue);
        Consumer consumer = new Consumer(queue);

        new Thread(producer).start();

        new Thread(consumer).start();

    }
}
