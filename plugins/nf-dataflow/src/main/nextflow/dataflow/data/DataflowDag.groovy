package nextflow.dataflow.data

import groovy.util.logging.Slf4j
import nextflow.dag.DAG
import nextflow.dataflow.helper.BaseScriptHelper
import nextflow.dataflow.helper.DataflowWriteChannelHelper
import nextflow.dataflow.helper.InParamHelper
import nextflow.dataflow.helper.OutParamHelper
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfig
import nextflow.script.params.DefaultInParam
import nextflow.script.params.DefaultOutParam
import nextflow.script.params.InputsList
import nextflow.script.params.OutputsList
import nextflow.util.MemoryUnit

@Slf4j
class DataflowDag extends DAG {

    private final Map<TaskRun, DataflowVertex> vertices = new HashMap<>()
    private final boolean quoteName

    DataflowDag( String format ) {
        if ( format.toLowerCase() in ["html", "mmd"] ) {
            this.quoteName = true
        } else if ( format.toLowerCase() in ["dot", "gexf"] ) {
            this.quoteName = false
        } else {
            throw new IllegalArgumentException("Unsupported DAG format: ${format}")
        }
    }

    /**
     * This method is required as Groovy otherwise throws an error
     */
    String inputName0( InParamHelper value ) {
        return value.name
    }

    String quoteName( String name ) {
        if ( quoteName ) {
            return "\"${name}\""
        }
        return name
    }

    void addVertex( TaskRun task, long inputSize, Map<TaskRun, DependencyStats> dependencies, DependencyStats extern ) {
        String name = quoteName("${task.name} (${MemoryUnit.of(inputSize).toString()})")
        DataflowVertex vertice = new DataflowVertex( name )

        for (final Map.Entry<TaskRun, DependencyStats> entry in dependencies.entrySet()) {
            DataflowWriteChannelHelper channel = entry.value.toChannel()
            DataflowVertex from = vertices.get( entry.key )
            if ( from == null ) {
                log.warn "Could not find vertex for task ${entry.key.name} in DAG"
                continue
            }
            vertice.addInput( channel )
            from.addOutput( channel )
        }

        if ( extern.hasData() ) {
            DataflowWriteChannelHelper channel = extern.toChannel()
            vertice.addInput( channel )
        }

        this.vertices.put( task, vertice )
    }

    void generate() {
        for (final Map.Entry<TaskRun, DataflowVertex> entry in vertices.entrySet()) {
            DataflowVertex vertice = entry.value
            addProcessNode( vertice.name, vertice.getInputs(), vertice.getOutputs() )
        }
    }

    private static class DataflowVertex {

        private final String name
        private final InputsList inputs = new InputsList()
        private final OutputsList outputs = new OutputsList()

        DataflowVertex( String name ) {
            this.name = name
        }

        void addInput( DataflowWriteChannelHelper input ) {
            def param = new InParamHelper( input);
            inputs.add( param )
        }

        InputsList getInputs() {
            if ( !inputs ) {
                inputs.add( new DefaultInParam( new ProcessConfig( new BaseScriptHelper(), null ) ) )
            }
            return inputs
        }

        OutputsList getOutputs() {
            if ( !outputs ) {
                outputs.add( new DefaultOutParam( new ProcessConfig( new BaseScriptHelper(), null ) ) )
            }
            return outputs
        }

        void addOutput( DataflowWriteChannelHelper output ) {
            outputs.add( new OutParamHelper( output ) )
        }

    }

}
