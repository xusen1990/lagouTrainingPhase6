package com.lagou.demo;

import java.util.concurrent.BlockingQueue;

public class Producer implements Runnable{

    private BlockingQueue<KouZhao> queue;

    public Producer(BlockingQueue<KouZhao> queue){
        this.queue = queue;
    }

    private Integer index = 0;

    @Override
    public void run() {

        while(true){
            try {
                Thread.sleep(100);

                if(queue.remainingCapacity() <= 0){
                    // queue空间已用满
                    System.out.println("口罩已经堆积如山了，大家快来买");
                }else{
                    KouZhao kouZhao = new KouZhao();
                    kouZhao.setId(index++);
                    kouZhao.setType("N95");
                    System.out.println("正在生产第"+(index-1)+"号口罩");
                    queue.put(kouZhao);
                    System.out.println();
                    System.out.println("已经生产了口罩：" + queue.size() + "个");
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
