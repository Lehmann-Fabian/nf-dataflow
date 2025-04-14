package nextflow.datatrail.data

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun

import java.nio.file.Path

@Slf4j
@CompileStatic
class DatatrailStorage {

    private final DatatrailDag dag
    private final DataWriter inputFile
    private final DataWriter outputFile
    private final Path summaryFile
    private final String delimiter
    private final Map<Path, TaskStats> outputs = new HashMap<>()
    private final Map<TaskRun, TaskStats> taskStats = new HashMap<>()

    DatatrailStorage(DatatrailDag dag, DataWriter inputFile, DataWriter outputFile, Path summaryFile, String delimiter ) {
        this.dag = dag
        this.inputFile = inputFile
        this.outputFile = outputFile
        this.summaryFile = summaryFile
        this.delimiter = delimiter
    }

    void addInputs( TaskRun task, Collection<Path> inputs ) {
        long size = calculateSize(inputs)
        this.taskStats.put( task, new TaskStats( task, inputs.size(), size) )
        processInputs( task, inputs )
        inputFile?.addTask( task.name, task.hash.toString(), inputs )
    }

    private void processInputs( TaskRun task, Collection<Path> inputs ) {
        Map<TaskRun, DependencyStats> dependencies = new HashMap<>()
        DependencyStats extern = new DependencyStats()

        synchronized (this.outputs) {
            for (Path path in inputs) {
                Path currentPath = path
                // find creating task
                TaskStats fileStats
                do {
                    fileStats = this.outputs.get( currentPath )
                    currentPath = currentPath.parent
                } while( fileStats == null && currentPath != null )
                long size = calculateSize( path )
                if ( fileStats != null ) {
                    if ( !dependencies.containsKey( fileStats.task ) ) {
                        dependencies.put( fileStats.task, new DependencyStats() )
                    }
                    dependencies.get( fileStats.task ).addFile( size )
                    fileStats.addUsedBy( task )
                } else {
                    extern.addFile( size )
                }
            }
        }

        dag?.addVertex( task, dependencies, extern )

    }

    void addOutputs(TaskRun task, Collection<Path> outputs ) {
        TaskStats taskStats = taskStats.get( task )
        long outputSize = calculateSize( outputs )
        synchronized (this.outputs) {
            for (final Path path in outputs) {
                this.outputs.put( path, taskStats )
                taskStats.addOutputs( outputs.size(), outputSize )
            }
        }
        dag?.addOutputsToVertex( task, outputs, outputSize )
        outputFile?.addTask( task.name, task.hash.toString(), outputs )
    }

    void close() {
        inputFile?.close()
        outputFile?.close()
        if ( summaryFile ) {
            createSummary()
        }
    }

    static long calculateSize( Collection<Path> files ) {
        return files
                .parallelStream()
                .mapToLong(DatatrailStorage::calculateSize)
                .sum()
    }

    static long calculateSize( Path path ) {
        File file = path.toFile()
        return file.directory ? file.directorySize() : file.length()
    }

    private void createSummary() {
        summaryFile.parent.mkdirs()
        try (PrintWriter writer = new PrintWriter(summaryFile.toFile())){
            writer.println("task${delimiter}hash${delimiter}inputs${delimiter}inputSize${delimiter}outputs${delimiter}outputSize${delimiter}usedBy")
            taskStats.values().stream()
                    .sorted { a, b -> a.task.id <=> b.task.id }
                    .forEach { task ->
                        String s = "${task.task.name}${delimiter}${task.task.hash}${delimiter}"
                        s += "${task.inputs}${delimiter}${task.inputSize}${delimiter}"
                        s += "${task.outputs}${delimiter}${task.outputSize}${delimiter}"
                        s += "${task.getUsedByCount()}"
                        writer.println(s)
                    }
        } catch (IOException e) {
            log.error "Unable to write summary file: ${summaryFile.toUriString()}", e
        }
    }



}
