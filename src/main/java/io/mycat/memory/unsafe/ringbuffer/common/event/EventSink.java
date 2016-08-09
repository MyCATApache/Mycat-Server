package io.mycat.memory.unsafe.ringbuffer.common.event;

/**
 * Event槽接口
 *
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/29
 */
public interface EventSink<E> {
    /**
     * 申请下一个Sequence->申请成功则获取对应槽的Event->利用translator初始化并填充对应槽的Event->发布Event
     * @param translator translator用户实现，用于初始化Event，这里是不带参数Translator
     */
     void publishEvent(EventTranslator<E> translator);

    /**
     * 尝试申请下一个Sequence->申请成功则获取对应槽的Event->利用translator初始化并填充对应槽的Event->发布Event
     * 若空间不足，则立即失败返回
     * @param translator translator用户实现，用于初始化Event，这里是不带参数Translator
     * @return 成功true，失败false
     */
     boolean tryPublishEvent(EventTranslator<E> translator);

     <A> void publishEvent(EventTranslatorOneArg<E, A> translator, A arg0);

     <A> boolean tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0);

     <A, B> void publishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1);

     <A, B> boolean tryPublishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1);

     <A, B, C> void publishEvent(EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2);

     <A, B, C> boolean tryPublishEvent(EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2);

     void publishEvent(EventTranslatorVararg<E> translator, Object... args);

     boolean tryPublishEvent(EventTranslatorVararg<E> translator, Object... args);

    /**
     * 包括申请多个Sequence->申请成功则获取对应槽的Event->利用每个translator初始化并填充每个对应槽的Event->发布Event
     * @param translators
     */
     void publishEvents(EventTranslator<E>[] translators);

     void publishEvents(EventTranslator<E>[] translators, int batchStartsAt, int batchSize);

     boolean tryPublishEvents(EventTranslator<E>[] translators);

     boolean tryPublishEvents(EventTranslator<E>[] translators, int batchStartsAt, int batchSize);

     <A> void publishEvents(EventTranslatorOneArg<E, A> translator, A[] arg0);

     <A> void publishEvents(EventTranslatorOneArg<E, A> translator, int batchStartsAt, int batchSize, A[] arg0);

     <A> boolean tryPublishEvents(EventTranslatorOneArg<E, A> translator, A[] arg0);

     <A> boolean tryPublishEvents(EventTranslatorOneArg<E, A> translator, int batchStartsAt, int batchSize, A[] arg0);

     <A, B> void publishEvents(EventTranslatorTwoArg<E, A, B> translator, A[] arg0, B[] arg1);

     <A, B> void publishEvents(
            EventTranslatorTwoArg<E, A, B> translator, int batchStartsAt, int batchSize, A[] arg0,
            B[] arg1);

     <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<E, A, B> translator, A[] arg0, B[] arg1);

     <A, B> boolean tryPublishEvents(
            EventTranslatorTwoArg<E, A, B> translator, int batchStartsAt, int batchSize,
            A[] arg0, B[] arg1);

     <A, B, C> void publishEvents(EventTranslatorThreeArg<E, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2);

     <A, B, C> void publishEvents(
            EventTranslatorThreeArg<E, A, B, C> translator, int batchStartsAt, int batchSize,
            A[] arg0, B[] arg1, C[] arg2);

     <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<E, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2);

     <A, B, C> boolean tryPublishEvents(
            EventTranslatorThreeArg<E, A, B, C> translator, int batchStartsAt,
            int batchSize, A[] arg0, B[] arg1, C[] arg2);

     void publishEvents(EventTranslatorVararg<E> translator, Object[]... args);

     void publishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args);

     boolean tryPublishEvents(EventTranslatorVararg<E> translator, Object[]... args);

     boolean tryPublishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args);
}
