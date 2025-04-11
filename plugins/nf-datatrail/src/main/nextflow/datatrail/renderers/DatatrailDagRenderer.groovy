package nextflow.datatrail.renderers

import groovy.transform.CompileStatic
import nextflow.datatrail.data.DatatrailDag

import java.nio.file.Path

@CompileStatic
interface DatatrailDagRenderer {

    void renderDocument(DatatrailDag dag, Path file)

}