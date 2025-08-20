package com.processor.costprocessor.batch;

import com.processor.costprocessor.model.abnormal.AbnormalItemModel;
import com.processor.costprocessor.model.util.AlarmReqModel;
import com.processor.costprocessor.util.AlarmService;
import com.processor.costprocessor.util.CalDatetime;
import com.processor.costprocessor.util.NumericCalculator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class AbnormalProcess {

    @Autowired
    @Qualifier("sqlSessionBatch") // qulifier는 의존성 주입 대상이 2개 이상일 때 어떤 것을 사용할지 명시적으로 지정하는 애노테이션
    private SqlSessionFactory sqlSessionFactory; // DB와 통신하면서 SQL실행하고 트랜잭션을 관리하는 객체(보통 Config로 생성한 뒤, 세션을 열어서 DB 작업)

    @Autowired
    private CalDatetime calDatetime; // 시간 관련 Util 객체

    @Autowired
    private AlarmService alarmService; // 알람 서비스 객체

    @Autowired
    private NumericCalculator numericCalculator; // 지수 표기 숫자 처리 Util 객체

    @Bean
    public CustomerItemListener customerListener(){
        return new CustomerItemListener(); // 배치작업(대량 데이터 읽기, 가공, 적재 과정) 중 이벤트 발생 시 어떻게 대응할지 커스텀한 객체(각 과정마다 실패해서 건너뛰면 로그 남기도록 해놓음)
    }

    // 배치 처리 과정 : Job -> Step -> Chunk -> Paging (점점 더 작아지는 태스크 범위)

    @Bean
    public Job abnormalProcessJob(JobRepository jobRepository, Step abnormalProcessStep) {
        // 매개변수 : JobRepository는 Job의 실행 정보, 상태, 이력을 DB에 기록하기 위한 저장소 객체, Step은 Job안에서 수행될 배치 처리 단위
        return new JobBuilder("abnormalProcessJob", jobRepository) // abnormalProcessJob 이라는 이름의 객체 생성
                .start(abnormalProcessStep)
                .build();
    }

    @Bean
    @JobScope // Job 런타임(실행 시점)에 넘어오는 값(파라미터)을 쓸 수 있게 만드는 임시 Bean 스코프를 정의하는 도구
    public Step abnormalProcessStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        // PlatformTransactionManager는 Spring이 어떤 종류의 데이터 소스를 사용하든 간에 트랜잭션을 열고, 커밋하고, 롤백할 수 있게 해주는 통합 트랜잭션 관리자
        return new StepBuilder("abnormalProcessStep", jobRepository) // 하나의 Step을 정의하는 설정
                .<AbnormalItemModel, AbnormalItemModel>chunk(50, platformTransactionManager)
                .reader(abnormalReader()) // 데이터 읽기
                .processor(abnormalProcessor()) // 데이터 가공
                .writer(abnormalWriterItems()) // 데이터 쓰기
                .startLimit(2) // 실행 최대 횟수 제한, Job이 재시작되더라도 3회째부터는 실행되지 않음
                .faultTolerant()// 예외 발생 시 배치가 멈추지 않도록 예외 허용 설정
                .retry(Exception.class) // retry 대상 예외
                .retryLimit(2) // 최대 retry 횟수, 최대 2번까지 재시도
                .skip(Exception.class)// 스킵 대상 예외
                .skipPolicy(new AlwaysSkipItemSkipPolicy()) // 무조건 스킵 정책
                .listener(customerListener()) // Step 처리 중에 이벤트를 감지해서 실행할 수 있는 리스너 등록 (55번 줄 객체)
                .build(); // step 생성
    }

    @Bean
    @StepScope
    public MyBatisPagingItemReader<AbnormalItemModel> abnormalReader(){
        Map<String, Object> param = new HashMap<>();
        param.put("standardDT", calDatetime.getYesterDate().toString());
        param.put("subjectDTs", calDatetime.getLastMonthWeeklyDates().stream()
                .map(LocalDate::toString)
                .collect(Collectors.toList()));

        return new MyBatisPagingItemReaderBuilder<AbnormalItemModel>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("abnoraml.getAbnormalSubjectsCost")
                .parameterValues(param)
                .pageSize(50) // 한번에 50건씩 읽어옴
                .build();
    } // DB에서 "이상 비용 대상자"를 기준 날짜 범위로 조회해서, MyBatis로 50건씩 페이징 처리하며 읽어오는 Spring Batch용 Reader

    @Bean
    @StepScope
    public ItemProcessor<AbnormalItemModel, AbnormalItemModel> abnormalProcessor(){
        // 이상비용이 기준비용 대비 %를 확인하고 판단 기준에 따라 알림을 발송하는 함수
        LocalDate collect_dt = LocalDate.now();

        return item -> {
            double percent;
            if(item.getSubject_cost() != 0){
                percent = ((item.getStandard_cost() - item.getSubject_cost()) / item.getSubject_cost()) * 100;
            } else {
                percent = 0;
            }
            item.setPercentage_point(percent);

            if(percent >= 30){
                item.setAbnormal_rating("Critical");
            } else if(percent >= 20){
                item.setAbnormal_rating("Caution");
            } else if(percent >= 10){
                item.setAbnormal_rating("Warning");
            } else {
                return null;
            }

            item.setCollect_dt(collect_dt);
            String pctFormatter = String.format("%.2f", item.getPercentage_point());

            AlarmReqModel alarmReqModel = AlarmReqModel.builder()
                    .event_type("Abnormal")
                    .resource_id(item.getProduct_cd())
                    .resource_type(item.getProduct_cd())
                    .csp_type(item.getCsp_type())
                    .urgency("Warning")
                    .plan(item.getAbnormal_rating())
                    .note("지난달 비용(" + numericCalculator.parseExponentialFormat(item.getSubject_cost()) + " USD) 대비 이번달 비용(" + numericCalculator.parseExponentialFormat(item.getStandard_cost()) + " USD)이 " + pctFormatter + " % 발생했습니다.")
                    .project_cd(item.getProject_cd())
                    .build();

            try{
                alarmService.sendAlarm(alarmReqModel);
            } catch (Exception e){
                log.error("[Abnoramal] Fail to Send Alarm - Project : {}, Product : {}, due to : {}", item.getProject_cd(), item.getProduct_cd(), e.getMessage());
            }
            return item;
        };
    }

    @Bean
    public MyBatisBatchItemWriter<AbnormalItemModel> abnormalWriterItems() {
        return new MyBatisBatchItemWriterBuilder<AbnormalItemModel>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("abnoraml.insertDailyAbnoramlCost")
                .build();
    }
    // 이상비용 집계 내용을 DB에 쓰는 함수
}
