package com.zhugeio.etl.common.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisAbstractCommittable;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.doris.flink.sink.copy.DorisCopyCommittable;
import org.apache.doris.flink.sink.copy.DorisCopyCommitter;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带回调的 DorisSink
 *
 * 在 2PC commit 成功后触发回调，用于统计成功写入 Doris 的记录数
 *
 * 内部包含:
 * - CountingSerializer: 序列化时计数
 * - CallbackCommitter: commit 成功后触发回调
 */
public class CallbackDorisSink<T> implements StatefulSink<T, DorisWriterState>,
        TwoPhaseCommittingSink<T, DorisAbstractCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(CallbackDorisSink.class);

    private final DorisSink<T> delegateSink;
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions dorisExecutionOptions;
    private final CommitSuccessCallback callback;
    private final String tableName;
    private final CountingSerializer<T> countingSerializer;

    private CallbackDorisSink(DorisOptions dorisOptions,
                              DorisReadOptions dorisReadOptions,
                              DorisExecutionOptions dorisExecutionOptions,
                              DorisRecordSerializer<T> serializer,
                              CommitSuccessCallback callback,
                              String tableName) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.dorisExecutionOptions = dorisExecutionOptions;
        this.callback = callback;
        this.tableName = tableName;
        this.countingSerializer = new CountingSerializer<>(serializer);

        this.delegateSink = DorisSink.<T>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(dorisReadOptions)
                .setDorisExecutionOptions(dorisExecutionOptions)
                .setSerializer(countingSerializer)
                .build();
    }

    @Override
    public DorisAbstractWriter createWriter(InitContext context) throws IOException {
        return delegateSink.createWriter(context);
    }

    @Override
    public DorisAbstractWriter restoreWriter(InitContext context, Collection<DorisWriterState> recoveredState) throws IOException {
        return delegateSink.restoreWriter(context, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<DorisWriterState> getWriterStateSerializer() {
        return delegateSink.getWriterStateSerializer();
    }

    @Override
    public Committer<DorisAbstractCommittable> createCommitter() throws IOException {
        return new CallbackCommitter();
    }

    @Override
    @SuppressWarnings("unchecked")
    public SimpleVersionedSerializer<DorisAbstractCommittable> getCommittableSerializer() {
        return (SimpleVersionedSerializer<DorisAbstractCommittable>) delegateSink.getCommittableSerializer();
    }

    // ==================== 内部类: CountingSerializer ====================

    /**
     * 带计数功能的序列化器包装
     */
    private static class CountingSerializer<T> implements DorisRecordSerializer<T> {

        private final DorisRecordSerializer<T> delegate;
        private final AtomicLong counter = new AtomicLong(0);

        CountingSerializer(DorisRecordSerializer<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public DorisRecord serialize(T record) throws IOException {
            DorisRecord result = delegate.serialize(record);
            if (result != null) {
                counter.incrementAndGet();
            }
            return result;
        }

        @Override
        public void initial() {
            delegate.initial();
        }

        @Override
        public DorisRecord flush() {
            return delegate.flush();
        }

        long getAndResetCount() {
            return counter.getAndSet(0);
        }
    }

    // ==================== 内部类: CallbackCommitter ====================

    /**
     * 带回调的 Committer
     */
    private class CallbackCommitter implements Committer<DorisAbstractCommittable> {

        private final DorisCommitter streamLoadCommitter;
        private final DorisCopyCommitter copyCommitter;

        CallbackCommitter() {
            this.streamLoadCommitter = new DorisCommitter(dorisOptions, dorisReadOptions, dorisExecutionOptions);
            this.copyCommitter = new DorisCopyCommitter(dorisOptions, dorisExecutionOptions.getMaxRetries());
        }

        @Override
        public void commit(Collection<CommitRequest<DorisAbstractCommittable>> requests)
                throws IOException, InterruptedException {

            long countBeforeCommit = countingSerializer.getAndResetCount();

            // 分离不同类型的 committable
            List<CommitRequest<DorisCommittable>> streamLoadRequests = new ArrayList<>();
            List<CommitRequest<DorisCopyCommittable>> copyRequests = new ArrayList<>();

            for (CommitRequest<DorisAbstractCommittable> request : requests) {
                DorisAbstractCommittable committable = request.getCommittable();
                if (committable instanceof DorisCommittable) {
                    streamLoadRequests.add(new CommitRequestAdapter<>((DorisCommittable) committable, request));
                } else if (committable instanceof DorisCopyCommittable) {
                    copyRequests.add(new CommitRequestAdapter<>((DorisCopyCommittable) committable, request));
                }
            }

            // 提交
            if (!streamLoadRequests.isEmpty()) {
                streamLoadCommitter.commit(streamLoadRequests);
            }
            if (!copyRequests.isEmpty()) {
                copyCommitter.commit(copyRequests);
            }

            // commit 成功，触发回调
            if (callback != null && countBeforeCommit > 0) {
                try {
                    callback.onCommitSuccess(tableName, countBeforeCommit);
                    LOG.debug("Commit 成功: table={}, count={}", tableName, countBeforeCommit);
                } catch (Exception e) {
                    LOG.warn("回调执行失败: {}", e.getMessage());
                }
            }
        }

        @Override
        public void close() throws Exception {
            streamLoadCommitter.close();
            copyCommitter.close();
        }
    }

    // ==================== 内部类: CommitRequestAdapter ====================

    private static class CommitRequestAdapter<T> implements Committer.CommitRequest<T> {
        private final T committable;
        private final Committer.CommitRequest<?> original;

        CommitRequestAdapter(T committable, Committer.CommitRequest<?> original) {
            this.committable = committable;
            this.original = original;
        }

        @Override
        public T getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return original.getNumberOfRetries();
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            original.signalFailedWithKnownReason(t);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            original.signalFailedWithUnknownReason(t);
        }

        @Override
        public void retryLater() {
            original.retryLater();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void updateAndRetryLater(T newCommittable) {
            ((Committer.CommitRequest<Object>) original).updateAndRetryLater(newCommittable);
        }

        @Override
        public void signalAlreadyCommitted() {
            original.signalAlreadyCommitted();
        }
    }

    // ==================== Builder ====================

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private DorisOptions dorisOptions;
        private DorisReadOptions dorisReadOptions;
        private DorisExecutionOptions dorisExecutionOptions;
        private DorisRecordSerializer<T> serializer;
        private CommitSuccessCallback callback;
        private String tableName;

        public Builder<T> setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public Builder<T> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
            this.dorisReadOptions = dorisReadOptions;
            return this;
        }

        public Builder<T> setDorisExecutionOptions(DorisExecutionOptions dorisExecutionOptions) {
            this.dorisExecutionOptions = dorisExecutionOptions;
            return this;
        }

        public Builder<T> setSerializer(DorisRecordSerializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> setCommitCallback(CommitSuccessCallback callback) {
            this.callback = callback;
            return this;
        }

        public Builder<T> setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public CallbackDorisSink<T> build() {
            if (dorisOptions == null) {
                throw new IllegalArgumentException("DorisOptions is required");
            }
            if (dorisExecutionOptions == null) {
                throw new IllegalArgumentException("DorisExecutionOptions is required");
            }
            if (serializer == null) {
                throw new IllegalArgumentException("Serializer is required");
            }
            if (dorisReadOptions == null) {
                dorisReadOptions = DorisReadOptions.builder().build();
            }

            return new CallbackDorisSink<>(
                    dorisOptions,
                    dorisReadOptions,
                    dorisExecutionOptions,
                    serializer,
                    callback,
                    tableName
            );
        }
    }
}