package io.temporal.samples.springboot.propagation;

import com.google.common.collect.Lists;
import io.temporal.activity.ActivityOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@WorkflowImpl(taskQueues = "RowTaskQueue")
@Slf4j
public class ChunkedWorkflowImpl implements ChunkedWorkflow {
    private ChunkProcessingActivity activity =
            Workflow.newActivityStub(
                    ChunkProcessingActivity.class,
                    ActivityOptions.newBuilder()
                            .setStartToCloseTimeout(Duration.ofSeconds(2))
                            .setHeartbeatTimeout(Duration.ofSeconds(1))
                            .build());

    @Override
    public ProcessingResult propagate() {
        List<UUID> rowIds = readRowIds();
        log.info("workflow {} rows to process {}", rowIds.size(), rowIds);
        ProcessingResult result = new ProcessingResult();
        result.rowsRead = 0;
        result.rowsWritten = 0;

        List<List<UUID>> chunks = Lists.partition(rowIds, 5);
        chunks.forEach(chunk -> {
            ProcessingResult innerResult = activity.processRows(chunk.stream().map(UUID::toString).toList());
            result.rowsRead += innerResult.rowsRead;
            result.rowsWritten += innerResult.rowsWritten;
            result.rowsProcessed.addAll(innerResult.rowsProcessed);
        });
        log.info("workflow {} rows processed {}", rowIds.size(), result);

        return result;
    }

    private List<UUID> readRowIds() {
        List<UUID> result = new ArrayList<>();
        int numIds = 9;
        for (int i = 0; i < numIds; i++) {
            result.add(UUID.randomUUID());
        }
        return result;
    }
}
