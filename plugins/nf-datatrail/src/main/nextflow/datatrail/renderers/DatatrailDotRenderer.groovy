package nextflow.datatrail.renderers

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.datatrail.data.DatatrailDag
import nextflow.datatrail.helper.DistinctColorGenerator
import nextflow.datatrail.helper.StringHelper

import java.nio.file.Path
import java.util.regex.Pattern

@Slf4j
@CompileStatic
class DatatrailDotRenderer implements DatatrailDagRenderer {

    private final String name
    private final boolean plotDetails
    private final boolean plotExternalInputs
    private final boolean plotLegend
    private final boolean clusterByTag
    private final boolean showTagNames
    private final String rankDir
    private final List<Pattern> filterTasks

    /**
     * Create a render instance
     *
     * @param name The graph name used in the DOT format
     */
    DatatrailDotRenderer(String name,
                         String rankDir,
                         boolean plotDetails,
                         boolean plotExternalInputs,
                         boolean plotLegend,
                         boolean clusterByTag,
                         boolean showTagNames,
                         List<String> filterTasks
    ) {
        this.name = normalise(name)
        this.rankDir = rankDir
        this.plotDetails = plotDetails
        this.filterTasks = filterTasks.collect {Pattern.compile(it) }
        this.plotExternalInputs = plotExternalInputs
        this.plotLegend = plotLegend
        this.clusterByTag = clusterByTag
        this.showTagNames = showTagNames
    }

    private static String normalise(String str) { str.replaceAll(/[^0-9_A-Za-z]/,'') }

    @Override
    void renderDocument(DatatrailDag dag, Path file) {
        log.info( "Rendering datatrail graph to file: ${file.toUriString()}" )
        file.parent.mkdirs()
        file.text = renderNetwork(dag)
    }

    private static List<String> createLegend(Map<String, String> colorMapForTasks, String commonName) {
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

    private boolean filterProcess(DatatrailDag.Process process ) {
        String name = process.getProcessName()
        return filterTasks.any { it.matcher(name).matches() }
    }

    private boolean keepEdge(DatatrailDag.Edge edge ) {
        return (edge.from.isOrigin() || !filterProcess( edge.from as DatatrailDag.Process ))
                && !filterProcess( edge.to as DatatrailDag.Process )
    }

    @PackageScope
    String renderNetwork(DatatrailDag dag) {
        def result = []
        List<DatatrailDag.Process> processesToPlot = dag.vertices
                .findAll { it.isProcess() && !filterProcess(it as DatatrailDag.Process) }
                .collect { it as DatatrailDag.Process }
        Map<String, String> colorMapForTasks = createColorMapForTasks( processesToPlot )
        result << "digraph \"${this.name}\" {"
        result << "  rankdir=$rankDir;"
        result << "  ranksep=${plotDetails ? 1.0 : 0.4};"
        if ( plotExternalInputs ) {
            result.addAll( createExternalInputs( dag.vertices ) )
        }
        result << "  // Vertices"
        String commonName = findCommonName(dag)
        List<String> processes

        if ( clusterByTag ) {
            processes = processesToPlot
                    .groupBy { it.getTag() }
                    .collect { tag,values -> createTagSubgraph( tag, values, colorMapForTasks, commonName ) }
                    .flatten() as List<String>
        } else {
            processes = processesToPlot
                    .groupBy { it.getRank() }
                    .collect { rank, values -> createRankSubgraph( rank, values, colorMapForTasks, commonName ) }
                    .flatten() as List<String>
        }
        result.addAll( processes )

        result << "  // Edges"
        List<DatatrailDag.Edge> edgesToRender = dag.edges
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

    private List<String> createExternalInputs( List<DatatrailDag.Vertex> vertices ) {
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

    private List<String> createRankSubgraph(int rank, List<DatatrailDag.Process> vertices, Map<String, String> colorMapForTasks, String commonName ) {
        List<String> subgraph = new LinkedList<>()
        subgraph << "  subgraph \"Rank: $rank\" {".toString()
        subgraph << "    rank = same;"
        List<String> verticesStrings = vertices.collect { renderProcess(it, colorMapForTasks, commonName).indent(2) }
        subgraph.addAll(verticesStrings)
        subgraph << "  }"
        return subgraph
    }

    private String createTagName(String tag ) {
        return showTagNames ? "cluster_${normalise( tag )}" : tag
    }

    private List<String> createTagSubgraph(String tag, List<DatatrailDag.Process> vertices, Map<String, String> colorMapForTasks, String commonName ) {
        List<String> subgraph = new LinkedList<>()
        if ( tag != null ) {
            subgraph << "  subgraph \"${createTagName(tag)}\" {".toString()
            subgraph << "    label=\"$tag\";".toString()
            subgraph << "    style=\"rounded\";"
        }
        List<String> verticesStrings = vertices
                .collect { renderProcess(it, colorMapForTasks, commonName).indent( tag == null ? 2 : 4 ) }
        subgraph.addAll(verticesStrings)
        if ( tag != null ) {
            subgraph << "  }"
        }
        return subgraph
    }

    private static Map<String, String> createColorMapForTasks( List<DatatrailDag.Process> vertices ) {
        Map<String, String> colorMap = [:]

        // Sorting guarantees that the same task name will always get the same color if the workflow does not change
        List<String> taskNames = vertices
                .collect {(it as DatatrailDag.Process).getProcessName() }
                .unique()
                .sort()
        List<String> colors = DistinctColorGenerator.generateDistinctColors(taskNames.size())

        int i = 0
        for( String taskName : taskNames ) {
            colorMap[taskName] = colors[i++]
        }

        return colorMap
    }

    private static String findCommonName(DatatrailDag dag ) {
        List<String[]> taskNames = dag.vertices
                .findAll { it.isProcess() }
                .collect { (it as DatatrailDag.Process).getProcessName() }
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

    private String renderOrigin(DatatrailDag.Vertex vertex) {
        List attrs = []
        attrs << "shape=point"
        attrs << "label=\"\""
        attrs << "fixedsize=true"
        attrs << "width=0.1"
        attrs << "tooltip=\"External data input\""
        return "  ${vertex.getID()} [${attrs.join(',')}];"
    }

    private String renderProcess(DatatrailDag.Process process, Map<String, String> colorMapForTasks, String commonName = "") {
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

    private String renderEdge(DatatrailDag.Edge edge, Map<String, String> colorMapForTasks, long minimumEdgeWidth, long maximumEdgeWidth) {
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