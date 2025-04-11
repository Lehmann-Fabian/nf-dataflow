package nextflow.datatrail
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
import nextflow.datatrail.data.DataWriter
import nextflow.datatrail.data.DatatrailDag
import nextflow.datatrail.data.DatatrailStorage
import nextflow.datatrail.helper.DAGStorage
import nextflow.datatrail.helper.StringHelper
import nextflow.datatrail.renderers.DatatrailDagRenderer
import nextflow.datatrail.renderers.DatatrailDotRenderer
import nextflow.datatrail.renderers.DatatrailGraphvizRenderer
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.script.params.FileOutParam
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import java.nio.file.Path

@Slf4j
@CompileStatic
class DatatrailObserver implements TraceObserver {

    private final DatatrailDag dag
    private final Path dagFile
    private final String dagName
    private final String dagFormat
    private final Path persistFile

    private final String rankdir
    private final boolean detailed
    private final boolean plotExternalInputs
    private final boolean plotLegend
    private final boolean clusterByTag
    private final boolean showTagNames
    private final List<String> filterTasks


    private final Path inputFile
    private final Path outputFile
    private final Path summaryFile
    private final DatatrailStorage storage

    DatatrailObserver(Session session) {
        dagFile = session.config.navigate('datatrail.plot.file') as Path
        if ( dagFile != null ) {
            dagFormat = dagFile.getExtension().toLowerCase() ?: 'html'
            dagName = dagFile.getBaseName()
            boolean dagOverwrite = StringHelper.stringToBoolean(session.config.navigate('datatrail.overwrite'), false)
            // check file existence
            final attrs = FileHelper.readAttributes(dagFile)
            if( attrs ) {
                if( dagOverwrite && (attrs.isDirectory() || !dagFile.delete()) )
                    throw new AbortOperationException("Unable to overwrite existing DAG file: ${dagFile.toUriString()}")
                else if( !dagOverwrite )
                    throw new AbortOperationException("DAG file already exists: ${dagFile.toUriString()} -- enable `dag.overwrite` in your config file to overwrite existing DAG files")
            }
            dag = new DatatrailDag( dagFormat )

            rankdir = session.config.navigate('datatrail.plot.rankdir') as String ?: 'TB'
            detailed = StringHelper.stringToBoolean(session.config.navigate('datatrail.plot.detailed'), false )
            plotExternalInputs = StringHelper.stringToBoolean(session.config.navigate('datatrail.plot.external'), true )
            plotLegend = StringHelper.stringToBoolean(session.config.navigate('datatrail.plot.legend'), true )
            clusterByTag = StringHelper.stringToBoolean(session.config.navigate('datatrail.plot.cluster'), false )
            showTagNames = StringHelper.stringToBoolean(session.config.navigate('datatrail.plot.tagNames'), true )
            filterTasks = session.config.navigate('datatrail.plot.filter') as List<String> ?: new LinkedList<String>()

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
        persistFile = session.config.navigate('datatrail.persist') as Path
        inputFile = initializeDataCSV( session, 'Input' )
        outputFile = initializeDataCSV( session, 'Output' )
        summaryFile = initializeDataCSV( session, 'Summary' )
        String delimiter = session.config.navigate('datatrail.delimiter') as String ?: ';'
        storage = new DatatrailStorage(
                dag,
                inputFile ? new DataWriter(inputFile, delimiter) : null,
                outputFile ? new DataWriter(outputFile, delimiter) : null,
                summaryFile,
                delimiter
        )
    }

    private static Path initializeDataCSV(Session session, String name ) {
        Path file = session.config.navigate("datatrail.${name.toLowerCase()}") as Path
        if ( file != null ) {
            if ( file.getExtension().toLowerCase() != 'csv' )
                throw new AbortOperationException("$name file must be a CSV file: ${file.toUriString()}")
            boolean inputOverwrite = StringHelper.stringToBoolean( session.config.navigate('datatrail.overwrite'), false )
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

    DatatrailDagRenderer createRender() {
        DatatrailDotRenderer renderer = new DatatrailDotRenderer(dagName, rankdir, detailed, plotExternalInputs, plotLegend, clusterByTag, showTagNames, filterTasks)
        return dagFormat == 'dot' ? renderer : new DatatrailGraphvizRenderer( dagFormat, renderer )
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
