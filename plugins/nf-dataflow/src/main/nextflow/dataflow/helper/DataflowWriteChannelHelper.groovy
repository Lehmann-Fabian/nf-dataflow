package nextflow.dataflow.helper

import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel

class DataflowWriteChannelHelper implements DataflowWriteChannel {

    private final String name

    DataflowWriteChannelHelper( String name ) {
        this.name = name
    }

    @Override
    DataflowWriteChannel leftShift(Object value) {
        return null
    }

    @Override
    void bind(Object value) {

    }

    @Override
    DataflowWriteChannel leftShift(DataflowReadChannel ref) {
        return null
    }

    String getName() {
        return name
    }

}
