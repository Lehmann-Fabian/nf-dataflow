package nextflow.dataflow
/*
 * Copyright 2021, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dataflow.data.DataWriter
import nextflow.dataflow.data.DataflowDag
import nextflow.dataflow.data.DataflowStorage
import nextflow.dataflow.helper.DAGStorage
import nextflow.dataflow.renderers.DagRenderer
import nextflow.dataflow.renderers.DataflowDotRenderer
import nextflow.dataflow.renderers.DataflowGraphvizRenderer
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.script.params.FileOutParam
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import java.nio.file.Path

@Slf4j
@CompileStatic
class DataflowObserver implements TraceObserver {

    private final DataflowDag dag
    private final Path dagFile
    private final String dagName
    private final String dagFormat
    private final Path persistFile

    private final boolean detailed
    private final boolean plotExternalInputs
    private final boolean plotLegend
    private final boolean clusterByTag
    private final boolean showTagNames
    private final List<String> filterTasks


    private final Path inputFile
    private final Path outputFile
    private final Path summaryFile
    private final DataflowStorage storage

    DataflowObserver(Session session) {
        dagFile = session.config.navigate('dataflow.dag') as Path
        if ( dagFile != null ) {
            dagFormat = dagFile.getExtension().toLowerCase() ?: 'html'
            dagName = dagFile.getBaseName()
            boolean dagOverwrite = session.config.navigate('dataflow.overwrite') as boolean
            // check file existence
            final attrs = FileHelper.readAttributes(dagFile)
            if( attrs ) {
                if( dagOverwrite && (attrs.isDirectory() || !dagFile.delete()) )
                    throw new AbortOperationException("Unable to overwrite existing DAG file: ${dagFile.toUriString()}")
                else if( !dagOverwrite )
                    throw new AbortOperationException("DAG file already exists: ${dagFile.toUriString()} -- enable `dag.overwrite` in your config file to overwrite existing DAG files")
            }
            dag = new DataflowDag( dagFormat )

            detailed = session.config.navigate('dataflow.plot.detailed') as boolean
            plotExternalInputs = session.config.navigate('dataflow.plot.external') as boolean
            plotLegend = session.config.navigate('dataflow.plot.legend') as boolean
            clusterByTag = session.config.navigate('dataflow.plot.cluster') as boolean
            showTagNames = session.config.navigate('dataflow.plot.tagNames') as boolean
            filterTasks = session.config.navigate('dataflow.plot.filter') as List<String>

        } else {
            dag = null
            dagName = null
            dagFormat = null
            detailed = false
            plotExternalInputs = false
            plotLegend = false
            clusterByTag = false
            showTagNames = false
            filterTasks = null
        }
        persistFile = session.config.navigate('dataflow.persist') as Path
        inputFile = initializeDataCSV( session, 'Input' )
        outputFile = initializeDataCSV( session, 'Output' )
        summaryFile = initializeDataCSV( session, 'Summary' )
        String delimiter = session.config.navigate('dataflow.delimiter') as String ?: ';'
        storage = new DataflowStorage(
                dag,
                inputFile ? new DataWriter(inputFile, delimiter) : null,
                outputFile ? new DataWriter(outputFile, delimiter) : null,
                summaryFile,
                delimiter
        )
    }

    private static Path initializeDataCSV(Session session, String name ) {
        Path file = session.config.navigate("dataflow.${name.toLowerCase()}") as Path
        if ( file != null ) {
            if ( file.getExtension().toLowerCase() != 'csv' )
                throw new AbortOperationException("$name file must be a CSV file: ${file.toUriString()}")
            boolean inputOverwrite = session.config.navigate('dataflow.overwrite') as boolean
            // check file existence
            final attrs = FileHelper.readAttributes(file)
            if( attrs ) {
                if( inputOverwrite && (attrs.isDirectory() || !file.delete()) )
                    throw new AbortOperationException("Unable to overwrite existing ${name.toLowerCase()} file: ${file.toUriString()}")
                else if( !inputOverwrite )
                    throw new AbortOperationException("$name file already exists: ${file.toUriString()} -- enable `dag.overwrite` in your config file to overwrite existing ${name.toLowerCase()} files")
            }
        }
        return file
    }

    @Override
    void onFlowComplete() {
        if ( dag ) {
            dag.generateNames()
            createRender().renderDocument(dag,dagFile)
            if ( persistFile ) {
                new DAGStorage(dag).persist(persistFile)
            }
        }
        storage.close()
    }

    DagRenderer createRender() {
        DataflowDotRenderer renderer = new DataflowDotRenderer(dagName, detailed, plotExternalInputs, plotLegend, clusterByTag, showTagNames, filterTasks)
        return dagFormat == 'dot' ? renderer : new DataflowGraphvizRenderer( dagFormat, renderer )
    }

    @Override
    void onProcessPending(TaskHandler handler, TraceRecord trace) {
        Map<String, Path> filesMap = handler.task.getInputFilesMap()
        Set<Path> inputFiles = filesMap ? filesMap.values() as Set<Path> : new HashSet<Path>()
        storage.addInputs( handler.task, inputFiles )
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        List<Path> outputFiles = handler.task.getOutputsByType(FileOutParam)
                .values()
                .flatten()
                .unique() as List<Path>
        storage.addOutputs(handler.task, outputFiles)
    }

}
