package com.github.feng111627.webhookingestdispatchjava.store;

import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

/** 事件记录存储（本地内存实现） */
public interface EventStore {

    /**
     * 幂等占位：key 不存在时写入 initial 并返回 null；已存在（含处理中）时返回现有记录。
     * 用于重复与并发事件的"只处理一次"。
     */
    EventRecord claimIfAbsent(String eventId, UnaryOperator<EventRecord> initialSupplier);

    Optional<EventRecord> find(String eventId);

    /** 在锁内更新单条记录 */
    void update(String eventId, UnaryOperator<EventRecord> mutator);

    List<EventRecord> findDead();
}
