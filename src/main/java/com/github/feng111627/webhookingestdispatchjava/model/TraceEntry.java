package com.github.feng111627.webhookingestdispatchjava.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.time.Instant;

/**
 * 判定轨迹上的一条记录：同一事件的每一步判定（接收/命中重复/被拒/每次尝试与退避/进死信/重放）。
 *
 * <p>{@code seq} 是同一事件内部的严格递增序号，{@code at} 为发生时刻；
 * {@code phase} 为机读阶段，{@code decision} 为人读判定依据，{@code apiConclusion}
 * 为当时接口返回的结论，保证轨迹与响应对得上。
 */
@JsonDeserialize(builder = TraceEntry.Builder.class)
public record TraceEntry(
        long seq,
        String eventId,
        TracePhase phase,
        String decision,
        String apiConclusion,
        Integer attemptNo,
        Integer maxAttempts,
        String failureKind,
        String payloadSource,
        String replayId,
        Instant at) {

    public static Builder builder(String eventId, TracePhase phase) {
        return new Builder().eventId(eventId).phase(phase);
    }

    /** 轨迹条目构建器：seq/at 由存储层统一赋值，业务侧只填判定内容。 */
    @JsonPOJOBuilder
    public static final class Builder {
        private long seq;
        private String eventId;
        private TracePhase phase;
        private String decision;
        private String apiConclusion;
        private Integer attemptNo;
        private Integer maxAttempts;
        private String failureKind;
        private String payloadSource;
        private String replayId;
        private Instant at;

        public Builder seq(long v) { this.seq = v; return this; }
        public Builder eventId(String v) { this.eventId = v; return this; }
        public Builder phase(TracePhase v) { this.phase = v; return this; }
        public Builder decision(String v) { this.decision = v; return this; }
        public Builder apiConclusion(String v) { this.apiConclusion = v; return this; }
        public Builder attemptNo(Integer v) { this.attemptNo = v; return this; }
        public Builder maxAttempts(Integer v) { this.maxAttempts = v; return this; }
        public Builder failureKind(String v) { this.failureKind = v; return this; }
        public Builder payloadSource(String v) { this.payloadSource = v; return this; }
        public Builder replayId(String v) { this.replayId = v; return this; }
        public Builder at(Instant v) { this.at = v; return this; }

        public String eventId() { return eventId; }
        public TracePhase phase() { return phase; }

        public TraceEntry build() {
            return new TraceEntry(seq, eventId, phase, decision, apiConclusion,
                    attemptNo, maxAttempts, failureKind, payloadSource, replayId, at);
        }
    }
}
