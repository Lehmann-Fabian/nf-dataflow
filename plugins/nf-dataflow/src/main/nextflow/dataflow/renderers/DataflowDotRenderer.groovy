package nextflow.dataflow.renderers

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.dataflow.data.DataflowDag
import nextflow.dataflow.helper.DistinctColorGenerator
import nextflow.dataflow.helper.StringHelper

import java.nio.file.Path
import java.util.regex.Pattern

@Slf4j
@CompileStatic
class DataflowDotRenderer implements DagRenderer {

    private final String name
    private final boolean plotDetails
    private final boolean plotExternalInputs = false
    private final boolean plotLegend = false
    private final List<Pattern> filterTasks = new LinkedList<>()

    /**
     * Create a render instance
     *
     * @param name The graph name used in the DOT format
     */
    DataflowDotRenderer( String name, boolean plotDetails ) {
        this.name = normalise(name)
        this.plotDetails = plotDetails
        String regex = ".*MULTIQC"
        Pattern pattern = Pattern.compile(regex)
        filterTasks.add(pattern)
    }

    @PackageScope
    static String normalise(String str) { str.replaceAll(/[^0-9_A-Za-z]/,'') }

    @Override
    void renderDocument(DataflowDag dag, Path file) {
        log.info( "Rendering dataflow graph to file: ${file.toUriString()}" )
        file.text = renderNetwork(dag)
    }

    static List<String> createLegend(Map<String, String> colorMapForTasks, String commonName) {
        List<String> result = new LinkedList<>()
        result << "  // Legend"
        result << "  legend [shape=plain, margin=0.2, label=<"
        result << "    <table border=\"0\" cellborder=\"1\" cellspacing=\"0\">"
        result << "      <tr><td colspan=\"2\"><b>Tasks</b></td></tr>"
        colorMapForTasks.each { taskName, color ->
            result << "    <tr><td bgcolor=\"${color}\"></td><td>${taskName.substring( commonName.length() )}</td></tr>".toString()
        }
        result << "    </table>"
        result << "  >];"
        result << "  { rank=sink; legend; }"
        return result

    }

    private boolean filterProcess( DataflowDag.Process process ) {
        String name = process.getProcessName()
        return filterTasks.any { it.matcher(name).matches() }
    }

    private boolean keepEdge(DataflowDag.Edge edge ) {
        return !filterProcess( edge.from as DataflowDag.Process ) && !filterProcess( edge.to as DataflowDag.Process )
    }

    String renderNetwork(DataflowDag dag) {
        def result = []
        List<DataflowDag.Process> processesToPlot = dag.vertices
                .findAll { it.isProcess() && !filterProcess(it as DataflowDag.Process) }
                .collect { it as DataflowDag.Process }
        Map<String, String> colorMapForTasks = createColorMapForTasks( processesToPlot )
        result << "digraph \"${this.name}\" {"
        result << "  rankdir=TB;" // arguments: LR or TB
        result << "  ranksep=${plotDetails ? 1.0 : 0.4};"
        if ( plotExternalInputs ) {
            result.addAll( createExternalInputs( dag.vertices ) )
        }
        result << "  // Vertices"
        String commonName = findCommonName(dag)
        List<String> processes = processesToPlot
                .groupBy { it.getRank() }
                .collect { rank, values -> createRankSubgraph( rank, values, colorMapForTasks, commonName ) }
                .flatten() as List<String>
        result.addAll( processes )

        processes = processesToPlot
            .groupBy { it.getTag() }
            .findAll { e,v -> e != null }
            .collect { e,v -> createTagSubgraph( e, v ) }
            .flatten() as List<String>
        result.addAll( processes )

        result << "  // Edges"
        List<DataflowDag.Edge> edgesToRender = dag.edges
        if ( !plotExternalInputs ) {
            edgesToRender = edgesToRender.findAll { !it.from.isOrigin() }
        }
        if ( filterTasks ) {
            edgesToRender = edgesToRender.findAll{ keepEdge( it ) }
        }
        long maximumEdgeWidth = edgesToRender.collect {it.inputSize }.max()
        long minimumEdgeWidth = edgesToRender.collect {it.inputSize }.min()
        edgesToRender.each { edge -> result << renderEdge( edge, colorMapForTasks, minimumEdgeWidth, maximumEdgeWidth ) }
        if ( plotLegend ) {
            result.addAll( createLegend(colorMapForTasks, commonName) )
        }
        result << "}\n"
        return result.join('\n')
    }

    private List<String> createExternalInputs( List<DataflowDag.Vertex> vertices ) {
        List<String> result = new LinkedList<>()
        List<String> inputs = vertices
                .findAll { it.isOrigin() }
                .collect { renderOrigin(it) }
        if (inputs) {
            result << "  // Inputs"
            result << "  subgraph inputs {"
            result << "    rank = same;"
            result.addAll(inputs.collect { it ? "  $it" : null } as List<String>)
            result << "  }"
        }
        return result
    }

    List<String> createRankSubgraph( int rank, List<DataflowDag.Process> vertices, Map<String, String> colorMapForTasks, String commonName ) {
        List<String> subgraph = new LinkedList<>()
        subgraph << "  subgraph \"Rank: $rank\" {".toString()
        subgraph << "    rank = same;"
        List<String> verticesStrings = vertices.collect { renderProcess(it, colorMapForTasks, commonName) }
        subgraph.addAll(verticesStrings)
        subgraph << "  }"
        return subgraph
    }

    List<String> createTagSubgraph( String tag, List<DataflowDag.Process> vertices ) {
        List<String> subgraph = new LinkedList<>()
        subgraph << "  subgraph \"Tag: $tag\" {".toString()
        subgraph << "    label=\"Tag: $tag\";".toString()
        subgraph << "    " + vertices.collect { it.getID() } .join( ';' ) + ";"
        subgraph << "  }"
        return subgraph
    }

    private static Map<String, String> createColorMapForTasks( List<DataflowDag.Process> vertices ) {
        Map<String, String> colorMap = [:]

        // Sorting guarantees that the same task name will always get the same color if the workflow does not change
        List<String> taskNames = vertices
                .collect {(it as DataflowDag.Process).getProcessName() }
                .unique()
                .sort()
        List<String> colors = DistinctColorGenerator.generateDistinctColors(taskNames.size())

        int i = 0
        for( String taskName : taskNames ) {
            colorMap[taskName] = colors[i++]
        }

        return colorMap
    }

    private static String findCommonName( DataflowDag dag ) {
        List<String[]> taskNames = dag.vertices
                .findAll { it.isProcess() }
                .collect { (it as DataflowDag.Process).getProcessName() }
                .findAll { it != null }
                .unique()
                .collect { it.split(":") }
        return findCommonName(taskNames)
    }

    private static String findCommonName(List<String[]> taskNames) {
        String[] commonName = taskNames[0]
        int common = commonName.length
        int minSize = taskNames.collect { it.length }.min()
        if (minSize == 1) {
            return ""
        }
        for (int i = 1; i < taskNames.size(); i++) {
            String[] taskName = taskNames[i]
            for (int j = 0; j < common; j++) {
                if (j >= taskName.length || commonName[j] != taskName[j]) {
                    common = j
                }
            }
        }
        if (common == minSize) {
            common--
        }
        if (common <= 0) {
            return ""
        }
        return commonName[0..common - 1].join(":") + ":"
    }

    private static String createTableRow(String label, int files, long size) {
        if ( files == 0 ) {
            return ""
        }
        return "<tr><td align=\"left\">$label:</td><td>${StringHelper.formatFiles(files)}</td><td>${StringHelper.formatSize(size)}</td></tr>"
    }

    private String renderOrigin(DataflowDag.Vertex vertex) {
        List attrs = []
        attrs << "shape=point"
        attrs << "label=\"\""
        attrs << "fixedsize=true"
        attrs << "width=0.1"
        attrs << "tooltip=\"External data input\""
        return "  ${vertex.getID()} [${attrs.join(',')}];"
    }

    private String renderProcess(DataflowDag.Process process, Map<String, String> colorMapForTasks, String commonName = "") {
        List attrs = []
        if ( plotDetails ) {
            attrs << """\
                        label=< 
                          <table border="0" cellspacing="0">
                            <tr><td colspan="4"><b>${process.getTaskName().substring(commonName.length())}</b></td></tr>
                            ${createTableRow("Input", process.getInputFiles(), process.getInputSize())}
                            ${createTableRow("Output", process.getOutputFiles(), process.getOutputSize())}
                          </table>
                        >"""
                    .stripIndent().indent(4).substring(4)
        } else {
            attrs << "label=\"\""
            attrs << "shape=point"
            attrs << "width=0.2"
            String tooltipText = process.getTaskName().substring(commonName.length())
            if ( process.inputFiles ) {
                tooltipText += "\nInputs: ${StringHelper.formatFiles(process.getInputFiles())} ${StringHelper.formatSize(process.getInputSize())}"
            }
            if ( process.outputFiles ) {
                tooltipText += "\nOutputs: ${StringHelper.formatFiles(process.getOutputFiles())} ${StringHelper.formatSize(process.getOutputSize())}"
            }
            attrs << "tooltip=\"$tooltipText\""
        }
        attrs << "style=filled"
        attrs << "fillcolor=\"${colorMapForTasks[process.getProcessName()]}\""
        return "${process.getID()} [${attrs.join(',')}];"
    }

    private String renderEdge(DataflowDag.Edge edge, Map<String, String> colorMapForTasks, long minimumEdgeWidth, long maximumEdgeWidth) {
        assert edge.from != null && edge.to != null
        def result = new StringBuilder()
        result << "  ${edge.from.ID} -> ${edge.to.ID}"
        List attrs = []
        if( edge.label && plotDetails ) {
            attrs << "label=\"${StringHelper.formatFiles(edge.getInputFiles())} ${StringHelper.formatSize(edge.getInputSize())}\""
            attrs << "fontsize=10"
        } else if ( edge.label ) {
            attrs << "label=\"\""
            attrs << "tooltip=\"${StringHelper.formatFiles(edge.getInputFiles())} ${StringHelper.formatSize(edge.getInputSize())}\""
        }
        if ( edge.inputSize > 0 ) {
            double thickness = edge.inputSize / maximumEdgeWidth + 0.08
            attrs << "penwidth=${thickness * 8}"
        }
        String toColor = colorMapForTasks[edge.to?.getProcessName()]
        if ( toColor ) {
            attrs << "color=\"${toColor}\""
        }
        if ( attrs ) {
            result << " [${attrs.join(',')}]"
        }
        result << ";"
    }

}