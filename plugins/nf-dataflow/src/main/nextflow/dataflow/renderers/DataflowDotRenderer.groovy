package nextflow.dag

import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.dataflow.helper.DistinctColorGenerator

import java.nio.file.Path;

@Slf4j
class DataflowDotRenderer implements DagRenderer {

    private String name

    /**
     * Create a render instance
     *
     * @param name The graph name used in the DOT format
     */
    DataflowDotRenderer(String name ) {
        this.name = normalise(name)
    }

    @PackageScope
    static String normalise(String str) { str.replaceAll(/[^0-9_A-Za-z]/,'') }

    @Override
    void renderDocument(DAG dag, Path file) {
        log.info( "Rendering dataflow graph to file: ${file.toUriString()}" )
        file.text = renderNetwork(dag)
    }

    static List<String> createLegend(Map<String, String> colorMapForTasks, String commonName) {
        def result = []
        result << "  // Legend"
        result << "  legend [shape=plain, margin=0.2, label=<"
        result << "    <table border=\"0\" cellborder=\"1\" cellspacing=\"0\">"
        result << "      <tr><td colspan=\"2\"><b>Tasks</b></td></tr>"
        colorMapForTasks.each { taskName, color ->
            result << "    <tr><td bgcolor=\"${color}\"></td><td>${taskName.substring( commonName.length() )}</td></tr>"
        }
        result << "    </table>"
        result << "  >];"
        result << "  { rank=sink; legend; }"
        return result

    }

    String renderNetwork(DAG dag) {
        def result = []
        Map<String, String> colorMapForTasks = createColorMapForTasks(dag)
        result << "digraph \"$name\" {"
        result << "  rankdir=TB;" // arguments: LR or TB
        result << "  ranksep=1.0;"
        List<String> inputs = dag.vertices
                .findAll { it.type == DAG.Type.ORIGIN }
                .collect { renderVertex( it, colorMapForTasks ) }
        if ( inputs ) {
            result << "  // Inputs"
            result << "  subgraph inputs {"
            result << "    rank = same;"
            result.addAll( inputs.collect { it ? "  $it" : null } )
            result << "  }"
        }
        result << "  // Vertices"
        String commonName = findCommonName(dag)
        List<String> processes = dag.vertices
                .findAll { it.type == DAG.Type.PROCESS }
                .collect { renderVertex( it, colorMapForTasks, commonName ) }
        result.addAll( processes )
        result << "  // Edges"
        dag.edges.each { edge -> result << renderEdge( edge, colorMapForTasks ) }
        result.addAll( createLegend(colorMapForTasks, commonName) )
        result << "}\n"
        return result.join('\n')
    }

    private static String extractTaskName(String name) {
        if ( !name ) return null
        return name.split(" ")[0]
    }

    private static Map<String, String> createColorMapForTasks( DAG dag ) {
        Map<String, String> colorMap = [:]

        // Sorting guarantees that the same task name will always get the same color if the workflow does not change
        List<String> taskNames = dag.vertices
                .collect { extractTaskName(it.label) }
                .findAll { it != null }
                .unique()
                .sort()
        List<String> colors = DistinctColorGenerator.generateDistinctColors(taskNames.size())

        int i = 0
        for( String taskName : taskNames ) {
            colorMap[taskName] = colors[i++]
        }

        return colorMap
    }

    private static String findCommonName( DAG dag ) {
        List<String[]> taskNames = dag.vertices
                .collect { extractTaskName(it.label) }
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

    private static String createTableRow(String label, String data ) {
        String text
        if ( data.startsWith( label) ) {
            text = data.replace("$label: ", "").split(" ").join( '</td><td>')
            text = "<td>$text</td>"
        } else {
            text = "<td colspan=\"3\">$data</td>"
        }
        return "<tr><td align=\"left\">$label:</td>${text}</tr>"
    }

    private static String renderVertex(DAG.Vertex vertex, Map<String, String> colorMapForTasks, String commonName = "") {

        List attrs = []

        switch (vertex.type) {
            case DAG.Type.NODE:
                attrs << "shape=point"
                if (vertex.label) {
                    attrs << "label=\"\""
                    attrs << "xlabel=\"$vertex.label\""
                }
                break

            case DAG.Type.ORIGIN:
                attrs << "shape=point"
                attrs << "label=\"\""
                attrs << "fixedsize=true"
                attrs << "width=0.1"
                if( vertex.label ) {
                    attrs << "xlabel=\"$vertex.label\""
                }
                break

            case DAG.Type.OPERATOR:
                attrs << "shape=circle"
                attrs << "label=\"\""
                attrs << "fixedsize=true"
                attrs << "width=0.1"
                if( vertex.label ) {
                    attrs << "xlabel=\"$vertex.label\""
                }
                break

            case DAG.Type.PROCESS:
                if( vertex.label ) {
                    String[] text = vertex.label.split("\n") // First: name, second: Input, third: Output
                    attrs << """\
                                label=< 
                                  <table border="0" cellspacing="0">
                                    <tr><td colspan="4"><b>${text[0].substring( commonName.length() )}</b></td></tr>
                                    ${createTableRow("Input", text[1])}
                                    ${createTableRow("Output", text[2])}
                                  </table>
                                >"""
                            .stripIndent().indent(4).substring(4)
                    attrs << "style=filled"
                    attrs << "fillcolor=\"${colorMapForTasks[extractTaskName(vertex.label)]}\""
                }
                break

            default:
                attrs << "shape=none"
                if( vertex.label )
                    attrs << "label=\"$vertex.label\""
        }


        return attrs ? "  ${vertex.getName()} [${attrs.join(',')}];" : null
    }

    private static String renderEdge(DAG.Edge edge, Map<String, String> colorMapForTasks) {
        assert edge.from != null && edge.to != null
        def result = new StringBuilder()
        result << "  ${edge.from.name} -> ${edge.to.name}"
        List attrs = []
        if( edge.label ) {
            attrs << "label=\"${edge.label}\""
            attrs << "fontsize=10"
        }
        String toColor = colorMapForTasks[extractTaskName(edge.to?.label)]
        if ( toColor ) {
            attrs << "color=\"${toColor}\""
        }
        if ( attrs ) {
            result << " [${attrs.join(',')}]"
        }
        result << ";"
    }

}