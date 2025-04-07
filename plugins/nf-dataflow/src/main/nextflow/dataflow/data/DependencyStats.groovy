package nextflow.dataflow.data

import groovy.transform.CompileStatic

@CompileStatic
class DependencyStats {

    private long size = 0
    private int files = 0

    void addFile( long size ) {
        this.size += size
        this.files++
    }

    boolean hasData() {
        return files
    }

    long getSize() {
        return size
    }

    int getFiles() {
        return files
    }

}
