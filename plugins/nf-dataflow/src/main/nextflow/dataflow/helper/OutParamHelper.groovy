package nextflow.dataflow.helper

import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.script.params.OutParam

class OutParamHelper implements OutParam {

    private final DataflowWriteChannelHelper outChannel

    OutParamHelper( DataflowWriteChannelHelper outChannel ) {
        this.outChannel = outChannel
    }

    @Override
    String getName() {
        return outChannel.name
    }

    @Override
    DataflowWriteChannel getOutChannel() {
        return outChannel
    }

    @Override
    short getIndex() {
        return 0
    }

    @Override
    String getChannelEmitName() {
        return null
    }

    @Override
    String getChannelTopicName() {
        return null
    }

}
