package com.github.feng111627.webhookingestdispatchjava.store;

import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import java.util.List;

/** 判定轨迹存储：按事件标识追加并查询整条轨迹。 */
public interface TraceStore {

    /** 追加一条轨迹；seq 与发生时刻由实现统一赋值，返回带序号的最终条目。 */
    TraceEntry append(TraceEntry.Builder entry);

    /** 按事件标识读取整条轨迹（严格按 seq 升序）；事件无任何轨迹时返回空列表。 */
    List<TraceEntry> findByEventId(String eventId);
}
