package com.processor.costprocessor.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;

@Slf4j
public class CustomerItemListener implements SkipListener<Object, Object> {
    // SkipListener는 배치 처리 중 오류가 발생해 건너뛴 항목들을 감지하고 처리함
    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("Skipped during read due to: {}", t.getMessage(), t);
        // 파일 읽다가 오류나면 건너뛰고 로그 남기기
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        log.warn("Skipped during write for item {} due to: {}", item, t.getMessage(), t);
        // 데이터 가공하다가 오류나면 건너뛰고 로그 남기기
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        log.warn("Skipped during process for item {} due to: {}", item, t.getMessage(), t);
        // DB에 데이터 쓰다가 오류나면 건너뛰고 로그 남기기
    }
}
