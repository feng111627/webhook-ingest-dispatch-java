package com.github.feng111627.webhookingestdispatchjava.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * 一次死信重放的历史记录：本次实际使用的负载（修正负载原文，未修正则为 null 表示沿用原始负载）、
 * 结论与尝试次数。原始负载永远保留在 {@link EventRecord#getPayload()}，不会被重放覆盖。
 */
public class ReplayRecord {

    private Instant startedAt;
    private Instant finishedAt;
    /** 本次重放实际使用的负载来源：ORIGINAL / CORRECTED */
    private String payloadSource;
    /** 人工修正后的负载原文（payloadSource=CORRECTED 时非空），用于事后核对改了什么 */
    private JsonNode correctedPayload;
    /** 重放结论：SUCCEEDED / DEAD */
    private String result;
    private int attemptsAfter;
    private String note;

    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }

    public Instant getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }

    public String getPayloadSource() { return payloadSource; }
    public void setPayloadSource(String payloadSource) { this.payloadSource = payloadSource; }

    public JsonNode getCorrectedPayload() { return correctedPayload; }
    public void setCorrectedPayload(JsonNode correctedPayload) { this.correctedPayload = correctedPayload; }

    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }

    public int getAttemptsAfter() { return attemptsAfter; }
    public void setAttemptsAfter(int attemptsAfter) { this.attemptsAfter = attemptsAfter; }

    public String getNote() { return note; }
    public void setNote(String note) { this.note = note; }
}
