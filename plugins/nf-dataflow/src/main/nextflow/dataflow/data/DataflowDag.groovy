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

import java.nio.file.Path

@Slf4j
class DataflowDag extends DAG {

    private final Map<TaskRun, DataflowVertex> vertices = new HashMap<>()
    private final boolean htmlNaming

    DataflowDag( String format ) {
        htmlNaming = format.toLowerCase() in ["html"]
    }

    /**
     * This method is required as Groovy otherwise throws an error
     */
    String inputName0( InParamHelper value ) {
        return value.name
    }

    private static String createInputString(long inputSize, Map<TaskRun, DependencyStats> dependencies, DependencyStats extern ) {
        if ( !dependencies && !extern.hasData() ) {
            return "No input files"
        }
        String inputSizeText = MemoryUnit.of(inputSize).toString().replace(" ", "")
        long dependencySize = dependencies ? dependencies.values().sum { it.files } as long : 0L
        long inputFiles = dependencySize + extern.files
        String inputFilesText = inputFiles > 1 ? 'files' : 'file'
        return "Input: $inputFiles $inputFilesText ($inputSizeText)"
    }

    void addVertex( TaskRun task, long inputSize, Map<TaskRun, DependencyStats> dependencies, DependencyStats extern ) {
        String name = task.name
        String inputText = createInputString( inputSize, dependencies, extern )
        DataflowVertex vertice = new DataflowVertex( name, inputText )

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
        } else if ( dependencies.isEmpty() ) {
            vertice.addInput( new DataflowWriteChannelHelper("No input files") )
        }

        this.vertices.put( task, vertice )
    }

    void generate() {
        for (final Map.Entry<TaskRun, DataflowVertex> entry in vertices.entrySet()) {
            DataflowVertex vertice = entry.value
            addProcessNode( vertice.getSyntheticName(), vertice.getInputs(), vertice.getOutputs() )
        }
    }

    void addOutputsToVertex(TaskRun taskRun, Collection<Path> paths, long outputSize) {
        String text
        int outputFiles
        if ( !paths ) {
            text = "No output files"
            outputFiles = 0
        } else {
            String sizeString = MemoryUnit.of( outputSize ).toString().replace( " ", "" )
            outputFiles = paths.size()
            String outputFilesText = outputFiles > 1 ? 'files' : 'file'
            text = "Output: $outputFiles $outputFilesText ($sizeString)"
        }
        vertices.get( taskRun ).setOutputData( text, outputFiles, outputSize )

    }

    private class DataflowVertex {

        private final String name
        private final String inputText
        private String outputText = null
        private final InputsList inputs = new InputsList()
        private final OutputsList outputs = new OutputsList()
        private int outputFiles = 0
        private long outputSize = 0

        DataflowVertex( String name, String inputText ) {
            this.name = name
            this.inputText = inputText
        }

        void addInput( DataflowWriteChannelHelper input ) {
            def param = new InParamHelper( input)
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

        int getOutputFiles() {
            return outputFiles
        }

        long getOutputSize() {
            return outputSize
        }

        void addOutput( DataflowWriteChannelHelper output ) {
            outputs.add( new OutParamHelper( output ) )
        }

        void setOutputData( String text, int outputFiles, long outputSize ) {
            outputText = text
            this.outputFiles = outputFiles
            this.outputSize = outputSize
        }

        String getSyntheticName() {
            return htmlNaming ? getHTMLName() : getNonHtmlName()
        }

        String getHTMLName() {
            return "<b>$name</b><br/>$inputText" + (outputText ? "<br/>$outputText" : "")
        }

        String getNonHtmlName() {
            return "$name\n$inputText" + (outputText ? "\n$outputText" : "")
        }

    }

}
