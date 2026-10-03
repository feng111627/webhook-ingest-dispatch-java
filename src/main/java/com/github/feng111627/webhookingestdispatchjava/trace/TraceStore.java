package com.github.feng111627.webhookingestdispatchjava.trace;

import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import java.util.List;

/** 事件轨迹存储：只追加、按 eventId 查询（本地内存实现） */
public interface TraceStore {

    /** 追加一条轨迹（eventId 为空时忽略，无法归键） */
    void append(TraceEntry entry);

    /** 按事件标识取出完整轨迹，按时间升序 */
    List<TraceEntry> findByEventId(String eventId);
}
