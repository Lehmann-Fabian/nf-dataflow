package dataflow.helper

import groovy.util.logging.Slf4j
import nextflow.dataflow.helper.DAGStorage
import nextflow.dataflow.renderers.DagRenderer
import nextflow.dataflow.renderers.DataflowDotRenderer
import nextflow.dataflow.renderers.DataflowGraphvizRenderer
import spock.lang.Specification

import java.nio.file.Path;
import java.nio.file.Paths
import java.util.regex.Pattern;

@Slf4j
class DAGStorageTest extends Specification {

    //static String root = "/home/fabian/test/groupMultiple"
    //static String root = "/mnt/c/Users/Fabian Lehmann/Documents/VBox/Paper/test/RSOptimize/dag/results/10gbit10nodes/chipseq/orig-ceph/1"
    static String root = "/mnt/c/Users/Fabian Lehmann/Documents/VBox/Paper/test/RSOptimize/dag/atacseqTest"

    Path load() {
        log.info root
        def dag = DAGStorage.load(Paths.get(root, "physicalDag.json"))
        def format = "svg"
        def file = Paths.get(root, "physicalDag.$format")

        final boolean plotDetails = false
        final boolean plotExternalInputs = false
        final boolean plotLegend = false
        final boolean clusterByTag = true
        final boolean showTagNames = true
        final List<Pattern> filterTasks

        DagRenderer renderer = format == 'dot'
                ? new DataflowDotRenderer("physicalDag", false)
                : new DataflowGraphvizRenderer( "physicalDag", format, false )
        renderer.renderDocument(dag, file)
        return file
    }


    def 'load Dag and write as dot' () {
        when:
        def f = load()
        then:
        f.exists()
    }

    def 'test Pattern' () {
        when:
        String regex = ".*MULTIQC"
        Pattern pattern = Pattern.compile(regex)
        then:
        pattern.matcher("NFCORE_ATACSEQ:ATACSEQ:MULTIQC").matches()
    }

}