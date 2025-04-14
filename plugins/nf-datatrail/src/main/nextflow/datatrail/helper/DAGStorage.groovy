package nextflow.datatrail.helper

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.datatrail.data.DatatrailDag

import java.nio.file.Path

@CompileStatic
@Slf4j
class DAGStorage {

    public final List<DatatrailDag.Origin> origins
    public final List<DatatrailDag.Process> processes = new LinkedList<>()
    public final List<DatatrailDag.Edge> edges = new LinkedList<>()

    DAGStorage(DatatrailDag dag) {
        origins = dag.vertices.findAll { it.isOrigin() }.collect{ it as DatatrailDag.Origin }
        processes = dag.vertices.findAll { it.isProcess() }.collect{ it as DatatrailDag.Process }
        edges = dag.edges
    }

    void persist( Path path ) {
        path.parent.mkdirs()
        path.withWriter { writer ->
            writer << JsonOutput.prettyPrint( JsonOutput.toJson(this) )
        }
    }

    @CompileDynamic
    static DatatrailDag load(Path path ) {
        JsonSlurper jsonSlurper = new JsonSlurper()
        Path data = path
        def text = jsonSlurper.parseText(data.text)

        List<Map<String, Object>> origins = text.origins
        List<Map<String, Object>> processes = text.processes
        List<Map<String, Object>> edges = text.edges

        List<DatatrailDag.Vertex> vertices = new ArrayList<>()
        for (Map<String, Object> origin : origins) {
            vertices.add(createOrigin(origin))
        }
        for (Map<String, Object> process : processes) {
            vertices.add(createProcess(process))
        }

        Map<String, DatatrailDag.Vertex> verticesMap = new HashMap<>()
        for (final DatatrailDag.Vertex v in vertices) {
            verticesMap.put( v.getID(), v )
        }

        List<DatatrailDag.Edge> edgesList = new ArrayList<>()
        for (Map<String, Object> edge : edges) {
            edgesList.add(createEdge( edge, verticesMap ))
        }

        DatatrailDag dag = new DatatrailDag("dot")
        dag.vertices.addAll( vertices )
        dag.edges.addAll( edgesList )
        return dag
    }

    static private DatatrailDag.Origin createOrigin(Map<String, Object> origin ) {
        String id = origin.get("ID")
        def origin1 = new DatatrailDag.Origin()
        origin1.setID(id)
        return origin1
    }

    static private DatatrailDag.Process createProcess(Map<String, Object> process ) {
        final String id = process.get("ID")
        final int outputFiles = process.get( "outputFiles" ) as int
        final long outputSize = process.get( "outputSize" ) as long
        final int inputFiles = process.get( "inputFiles" ) as int
        final long inputSize = process.get( "inputSize" ) as long
        final String taskName = process.get( "taskName" ) as String
        final String processName = process.get( "processName" ) as String ?: taskName.split( " " )[0]
        def process1 = new DatatrailDag.Process( taskName, processName )
        process1.setID(id)
        process1.setOutputData( outputFiles, outputSize )
        process1.addInputData( inputFiles, inputSize )
        return process1
    }

    static private DatatrailDag.Edge createEdge(Map<String, Object> edge, Map<String, DatatrailDag.Vertex> verticesMap ) {
        final String id = edge.get("ID")
        final String from = (edge.get("from") as Map).get("ID") as String
        final String to = (edge.get("to") as Map).get("ID") as String
        final int inputFiles = edge.get( "inputFiles" ) as int
        final long inputSize = edge.get( "inputSize" ) as long
        def fromVertex = verticesMap.get(from)
        def toVertex = verticesMap.get(to)
        if ( fromVertex == null ) {
            log.error "From vertex not found: ${edge}"
            throw new IllegalStateException("From vertex not found: ${edge}")
        }
        if ( toVertex == null ) {
            log.error "To vertex not found: ${edge}"
            throw new IllegalStateException("To vertex not found: ${edge}")
        }
        def edge1 = new DatatrailDag.Edge(fromVertex, toVertex as DatatrailDag.Process )
        edge1.setInputData( inputFiles, inputSize )
        edge1.setID(id)
        return edge1
    }

}
