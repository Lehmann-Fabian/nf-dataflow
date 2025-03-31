package nextflow.dataflow.data

import nextflow.dataflow.helper.DataflowWriteChannelHelper
import nextflow.util.MemoryUnit

class DependencyStats {

    private long size = 0
    private int files = 0

    void addFile( long size ) {
        this.size += size
        this.files++
    }

    DataflowWriteChannelHelper toChannel() {
        String fileText = files > 1 ? 'files' : 'file'
        String size = new MemoryUnit(size).toString().replace(" ", "")
        String text = "$files $fileText ($size)"
        return new DataflowWriteChannelHelper(text)
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
