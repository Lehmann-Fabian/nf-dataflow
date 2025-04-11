package nextflow.datatrail.data

import groovy.transform.CompileStatic
import nextflow.processor.TaskRun

@CompileStatic
class TaskStats {

    private final TaskRun task
    private final Set<TaskRun> usedBy = new HashSet<>()

    private final int inputs
    private final long inputSize

    private int outputs
    private long outputSize

    TaskStats(TaskRun task, int inputs, long inputSize) {
        this.task = task
        this.inputs = inputs
        this.inputSize = inputSize
    }

    void addUsedBy(TaskRun task) {
        synchronized ( usedBy ) {
            usedBy.add(task)
        }
    }

    void addOutputs(int outputs, long outputSize) {
        this.outputs = outputs
        this.outputSize = outputSize
    }

    TaskRun getTask() {
        return task
    }

    int getInputs() {
        return inputs
    }

    long getInputSize() {
        return inputSize
    }

    int getOutputs() {
        return outputs
    }

    long getOutputSize() {
        return outputSize
    }

    int getUsedByCount() {
        return usedBy.size()
    }

}
