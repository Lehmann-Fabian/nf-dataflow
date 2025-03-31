package nextflow.dataflow.helper

import groovyx.gpars.dataflow.DataflowReadChannel
import nextflow.script.ProcessConfig
import nextflow.script.params.FileInParam

class InParamHelper extends FileInParam {

    private static final ProcessConfig processConfig = new ProcessConfig( new BaseScriptHelper(), null )
    private final DataflowWriteChannelHelper rawChannel

    InParamHelper( DataflowWriteChannelHelper rawChannel ) {
        super(processConfig)
        this.rawChannel = rawChannel
    }

    @Override
    String getName() {
        return rawChannel.name
    }

    @Override
    DataflowReadChannel getInChannel() {
        return null
    }

    @Override
    Object getRawChannel() {
        return rawChannel
    }

    @Override
    Object decodeInputs(List values) {
        return null
    }

}
