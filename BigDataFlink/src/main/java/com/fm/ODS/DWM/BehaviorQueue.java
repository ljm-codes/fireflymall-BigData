package com.fm.ODS.DWM;

import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Data
public class BehaviorQueue {

    private LinkedList<DetailedBehavior> queue = new LinkedList<>();
    private int maxSize;

    public BehaviorQueue(int maxSize) {
        this.maxSize = maxSize;
    }

    public void add(DetailedBehavior behavior) {
        queue.addLast(behavior);    //  将最新消息添加到队列尾部
        while (queue.size() > maxSize) {
            queue.removeFirst();    //  移除最早的消息
        }
    }

    public List<DetailedBehavior> getAll() {
        return new ArrayList<>(queue);
    }

    public List<DetailedBehavior> getRecent(int num) {
        return queue.subList(0, Math.min(num, queue.size()));
    }

    public int getSize() {
        return queue.size();
    }

}
