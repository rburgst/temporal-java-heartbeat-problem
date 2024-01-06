package io.temporal.samples.springboot.propagation;

import io.temporal.activity.ActivityInterface;

import java.util.List;

@ActivityInterface
public interface ChunkProcessingActivity {

  ChunkedWorkflow.ProcessingResult processRows(List<String> rowId);
}
