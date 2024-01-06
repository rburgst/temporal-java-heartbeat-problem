package io.temporal.samples.springboot.propagation;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@WorkflowInterface
public interface ChunkedWorkflow {

  @ToString
  public static final class ProcessingResult {
    public int rowsRead;
    public int rowsWritten;
    List<String> rowsProcessed = new ArrayList<>();

    void addRowId(String rowId) {
      rowsProcessed.add(rowId);
    }
  }

  @WorkflowMethod
  ProcessingResult propagate();
}
