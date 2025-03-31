package nextflow.dataflow.data

import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun
import nextflow.util.MemoryUnit

import java.nio.file.Path

@Slf4j
class DataflowStorage {

    private final DataflowDag dag
    private final Map<Path, TaskRun> outputs = new HashMap<>()

    DataflowStorage( DataflowDag dag ) {
        this.dag = dag
    }


    void addInputs( TaskRun task, Collection<Path> inputs ) {
        if ( dag ) {
            inputsToDag( task, inputs )
        }
    }

    private void inputsToDag( TaskRun task, Collection<Path> inputs ) {

        long inputSize = calculateSize( inputs )
        Map<TaskRun, DependencyStats> dependencies = new HashMap<>()
        DependencyStats extern = new DependencyStats()

        synchronized (this.outputs) {
            for (Path path in inputs) {
                Path currentPath = path
                // find creating task
                TaskRun dependentTask
                do {
                    dependentTask = this.outputs.get( currentPath )
                } while( task == null && (currentPath = path.parent) != null )
                long size = calculateSize( path )
                if ( dependentTask != null ) {
                    if ( !dependencies.containsKey( dependentTask) ) {
                        dependencies.put( dependentTask, new DependencyStats() )
                    }
                    dependencies.get( dependentTask ).addFile( size )
                } else {
                    extern.addFile( size )
                }
            }
        }

        dag?.addVertex( task, inputSize, dependencies, extern )

    }

    void addOutputs(TaskRun task, Collection<Path> outputs ) {
        long outputSize = calculateSize( outputs )
        MemoryUnit size = MemoryUnit.of( outputSize )
        synchronized (this.outputs) {
            for (final Path path in outputs) {
                this.outputs.put( path, task )
            }
        }
    }

    private static long calculateSize( Collection<Path> files ) {
        return files
                .parallelStream()
                .mapToLong(DataflowStorage::calculateSize)
                .sum()
    }

    private static long calculateSize( Path path ) {
        File file = path.toFile()
        return file.directory ? file.directorySize() : file.length()
    }



}
