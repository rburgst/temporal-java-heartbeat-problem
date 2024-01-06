package io.temporal.samples.springboot.propagation;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.spring.boot.ActivityImpl;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.temporal.samples.springboot.propagation.ChunkedWorkflow.ProcessingResult;

@Component
@ActivityImpl(taskQueues = "RowTaskQueue")
public class ChunkProcessingActivityImpl implements ChunkProcessingActivity {
    private static final Logger log = LoggerFactory.getLogger(ChunkProcessingActivityImpl.class);

    @AllArgsConstructor
    @NoArgsConstructor
    public static final class HeartbeatData {
        List<String> remaining;
        ProcessingResult progressSoFar;
    }

    @Override
    public ProcessingResult processRows(List<String> rowId) {

        ActivityExecutionContext context = Activity.getExecutionContext();
        Optional<HeartbeatData> heartbeatDetails = context.getHeartbeatDetails(HeartbeatData.class);

        ProcessingResult result =
                new ProcessingResult();
        result.rowsRead = 0;
        result.rowsWritten = 0;

        HeartbeatData heartbeatData = heartbeatDetails.orElse(new HeartbeatData(rowId, result));
        List<String> remainingUuids = heartbeatData.remaining;
        result = heartbeatData.progressSoFar;
        log.info("propagating {}, heartbeat {}, rows {}", remainingUuids.size(), heartbeatDetails, remainingUuids);

        int index = 0;

        List<String> newRemaining = new ArrayList<>(remainingUuids);
        for (String uuid : remainingUuids) {
            if (index == 4) {
                throw new RuntimeException("foo error for uuid " + uuid);
            }
            newRemaining.remove(uuid);
            ProcessingResult innerResult = processSingleRow(UUID.fromString(uuid));
            log.info("   sending heartbeat with remaining {}, {}", newRemaining.size(), newRemaining);
            result.rowsRead += innerResult.rowsRead;
            result.rowsWritten += innerResult.rowsWritten;
            result.rowsProcessed.addAll(innerResult.rowsProcessed);
            context.heartbeat(new HeartbeatData(newRemaining, result));
            index++;
        }
        log.info("processing {} rows {} … done", remainingUuids.size(), remainingUuids);
        log.info("processing result {}", result);


        return result;
    }

    private ProcessingResult processSingleRow(UUID uuid) {

        ProcessingResult result =
                new ProcessingResult();
        result.rowsRead = 1;
        result.rowsWritten = 1;
        log.info("processing single row {}", uuid);
        result.addRowId(uuid.toString());
        return result;
    }
}
