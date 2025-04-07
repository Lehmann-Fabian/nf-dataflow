package nextflow.dataflow.helper

import groovy.transform.CompileStatic
import nextflow.util.MemoryUnit

@CompileStatic
class StringHelper {

    static String formatSize(long size) {
        return new MemoryUnit(size).toString().replace( " ", "" )
    }

    static String formatFiles(int files) {
        return files == 1 ? "1 file" : "$files files"
    }

}
