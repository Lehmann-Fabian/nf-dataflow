package nextflow.dataflow.renderers

import groovy.transform.CompileStatic
import nextflow.dataflow.data.DataflowDag

import java.nio.file.Path

@CompileStatic
interface DagRenderer {

    void renderDocument(DataflowDag dag, Path file)

}