package nextflow.dataflow.data


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun

import java.nio.file.Path

@Slf4j
@CompileStatic
class DataflowDag {

    private final Map<TaskRun, Process> taskToVertex = new HashMap<>()
    private final boolean htmlNaming

    private final List<Edge> edges = new ArrayList<>(50)
    private final List<Vertex> vertices = new ArrayList<>(50)

    DataflowDag( String format ) {
        htmlNaming = format.toLowerCase() in ["html"]
    }

    List<Vertex> getVertices() {
        return vertices
    }

    List<Edge> getEdges() {
        return edges
    }

    void generateNames() {
        int p = 0
        int o = 0
        for (final Vertex v in vertices) {
            final String id
            if ( v.isProcess() ) {
                id = "p${p++}"
            } else {
                id = "o${o++}"
            }
            v.setID( id )
        }
    }

    void addVertex(TaskRun task, Map<TaskRun, DependencyStats> dependencies, DependencyStats extern ) {
        String name = task.name
        Process to = new Process( name, task.processor.getName() )

        for (final Map.Entry<TaskRun, DependencyStats> entry in dependencies.entrySet()) {
            Process from = taskToVertex.get( entry.key )
            if ( from == null ) {
                log.warn "Could not find vertex for task ${entry.key.name} in DAG"
                continue
            }
            addEdge( from, to, entry.value )
        }

        if ( extern.hasData() ) {
            Origin origin = new Origin()
            vertices.add( origin )
            addEdge( origin, to, extern )
        }

        this.taskToVertex.put( task, to )
        vertices.add( to )
    }

    private void addEdge(Vertex from, Process to, DependencyStats stats ) {
        to.addInputData( stats.files, stats.size )
        Edge edge = new Edge( from, to )
        edge.setInputData( stats.files, stats.size )
        edges.add( edge )
    }

    void addOutputsToVertex(TaskRun taskRun, Collection<Path> paths, long outputSize) {
        taskToVertex.get( taskRun ).setOutputData( paths.size(), outputSize )
    }

    abstract static class Element {
        private String id

        void setID(String id) {
            this.id = id
        }

        String getID() {
            return id
        }
    }

    abstract static class Vertex extends Element {
        protected transient final List<Edge> outgoing = new LinkedList<>()
        abstract boolean isProcess()
        abstract boolean isOrigin()

        void addOutgoing(Edge edge) {
            outgoing.add(edge)
        }

        abstract int getRank()
    }

    static class Origin extends Vertex {

        Origin(){}

        @Override
        boolean isProcess() {
            return false
        }

        @Override
        boolean isOrigin() {
            return true
        }

        @Override
        int getRank() {
            return 0
        }
    }

    static class Process extends Vertex {

        private int outputFiles = 0
        private long outputSize = 0
        private int inputFiles = 0
        private long inputSize = 0
        private final String taskName
        private final String processName
        private int rank = -1
        private transient final List<Edge> incoming = new LinkedList<>()

        Process(String taskName, String processName) {
            this.taskName = taskName
            this.processName = processName
        }

        void addInputData(int inputFiles, long inputSize ) {
            this.inputFiles += inputFiles
            this.inputSize += inputSize
        }

        void setOutputData( int outputFiles, long outputSize ) {
            this.outputFiles = outputFiles
            this.outputSize = outputSize
        }

        String getTaskName() {
            return taskName
        }

        String getProcessName() {
            return processName
        }

        String getLabel() {
            return "$taskName input: $inputFiles files ($inputSize bytes)"
        }


        @Override
        boolean isProcess() {
            return true
        }

        @Override
        boolean isOrigin() {
            return false
        }

        int getInputFiles() {
            return inputFiles
        }

        long getInputSize() {
            return inputSize
        }

        int getOutputFiles() {
            return outputFiles
        }

        long getOutputSize() {
            return outputSize
        }

        void addIncoming(Edge edge) {
            incoming.add(edge)
        }

        @Override
        int getRank() {
            if ( rank == -1 && incoming.isEmpty() ) {
                rank = 1 // rank 1 is higher than rank 0 which is used for external data
            } else if ( rank == -1 ) {
                rank = incoming.collect { it.from.getRank() }.max() + 1
            }
            return rank
        }

        String getTag() {
            if ( taskName.contains( " (") ) {
                return taskName.split( " \\(" )[1]
            } else {
                return null
            }
        }
    }

    static class Edge extends Element {

        final Vertex from
        final Process to
        private int inputFiles = 0
        private long inputSize = 0

        Edge( Vertex from, Process to ) {
            this.from = from
            this.to = to
            from.addOutgoing( this )
            to.addIncoming( this )
        }

        void setInputData( int inputFiles, long inputSize ) {
            this.inputFiles = inputFiles
            this.inputSize = inputSize
        }

        String getLabel() {
            return "Input: $inputFiles files ($inputSize bytes)"
        }

        int getInputFiles() {
            return inputFiles
        }

        long getInputSize() {
            return inputSize
        }

    }

}
