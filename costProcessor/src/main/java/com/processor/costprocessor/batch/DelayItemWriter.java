package com.processor.costprocessor.batch;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;


public class DelayItemWriter<T> implements ItemWriter<T> {

    private final long delayInMillis;

    public DelayItemWriter(long delayInMillis) {
        this.delayInMillis = delayInMillis;
    }
    // 데이터를 저장하지 않고 일정 시간동안 Writer 시간 지연 -> 테스트 또는 흐름 제어 목적
    @Override
    public void write(Chunk<? extends T> items) throws Exception {
        Thread.sleep(delayInMillis);
    }
}
