package nextflow.dag

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.dataflow.data.DependencyStats
import nextflow.processor.TaskRun

import java.nio.file.Path

@Slf4j
@CompileStatic
class DataflowDag extends DAG {

    private final Map<TaskRun, DataflowVertex> taskToVertex = new HashMap<>()
    private final boolean htmlNaming

    DataflowDag( String format ) {
        htmlNaming = format.toLowerCase() in ["html"]
    }

    void addVertex(TaskRun task, Map<TaskRun, DependencyStats> dependencies, DependencyStats extern ) {
        String name = task.name
        DataflowVertex vertice = new DataflowVertex( name )

        for (final Map.Entry<TaskRun, DependencyStats> entry in dependencies.entrySet()) {
            DataflowVertex from = taskToVertex.get( entry.key )
            if ( from == null ) {
                log.warn "Could not find vertex for task ${entry.key.name} in DAG"
                continue
            }
            addEdge( from, vertice, entry.value )
        }

        if ( extern.hasData() ) {
            addEdge( new Vertex( Type.ORIGIN ), vertice, extern )
        }

        this.taskToVertex.put( task, vertice )
        getVertices().add( vertice )
    }

    private void addEdge( Vertex from, DataflowVertex to, DependencyStats stats ) {
        to.setInputData( stats.files, stats.size )
        DataflowEdge edge = new DataflowEdge( from: from, to: to )
        edge.setInputData( stats.files, stats.size )
        getEdges().add( edge )
    }

    void addOutputsToVertex(TaskRun taskRun, Collection<Path> paths, long outputSize) {
        taskToVertex.get( taskRun ).setOutputData( paths.size(), outputSize )
    }

    private class DataflowVertex extends DAG.Vertex {

        private int outputFiles = 0
        private long outputSize = 0
        private int inputFiles = 0
        private long inputSize = 0

        DataflowVertex( String label ) {
            super( DAG.Type.PROCESS, label )
        }


        void setInputData( int inputFiles, long inputSize ) {
            this.inputFiles = inputFiles
            this.inputSize = inputSize
        }

        void setOutputData( int outputFiles, long outputSize ) {
            this.outputFiles = outputFiles
            this.outputSize = outputSize
        }

//        if ( !paths ) {
//            text = "No output files"
//            outputFiles = 0
//        } else {
//            String sizeString = MemoryUnit.of( outputSize ).toString().replace( " ", "" )
//            outputFiles = paths.size()
//            String outputFilesText = outputFiles > 1 ? 'files' : 'file'
//            text = "Output: $outputFiles $outputFilesText ($sizeString)"
//        }

        String getLabel() {
            return label + "ByUs"
        }

    }

    @CompileDynamic
    class DataflowEdge extends DAG.Edge {

        private int inputFiles = 0
        private long inputSize = 0

        DataflowEdge(Map args) {
            super(args)
        }

        void setInputData( int inputFiles, long inputSize ) {
            this.inputFiles = inputFiles
            this.inputSize = inputSize
        }

    }

}
