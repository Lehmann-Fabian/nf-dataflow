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
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dag.DAG
import nextflow.dag.DagRenderer
import nextflow.dag.DotRenderer
import nextflow.dag.GexfRenderer
import nextflow.dag.GraphvizRenderer
import nextflow.dag.MermaidHtmlRenderer
import nextflow.dag.MermaidRenderer
import nextflow.dataflow.data.DataWriter
import nextflow.dataflow.data.DataflowDag
import nextflow.dataflow.data.DataflowStorage
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.script.params.FileOutParam
import nextflow.script.params.OutParam
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
        } else {
            dag = null
            dagName = null
            dagFormat = null
        }
        storage = new DataflowStorage( dag )
    }

    private static Path initializeDataCSV(Session session, String name ) {
        Path file = session.config.navigate('dataflow.input') as Path
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
            dag.generate()
            dag.normalize()
            log.info "Dataflow DAG: ${dagName} (${dagFormat}), ${dag.vertices}"
            log.info "Dataflow DAG: ${dagName} (${dagFormat}), ${dag.edges}"
            createRender().renderDocument(dag,dagFile)
        }
    }

    @PackageScope
    DagRenderer createRender() {
        if( dagFormat == 'dot' )
            new DotRenderer(dagName)

        else if( dagFormat == 'html' )
            new MermaidHtmlRenderer()

        else if( dagFormat == 'gexf' )
            new GexfRenderer(dagName)

        else if( dagFormat == 'mmd' )
            new MermaidRenderer()

        else
            new GraphvizRenderer(dagName, dagFormat)
    }

    @Override
    void onProcessPending(TaskHandler handler, TraceRecord trace) {
        Map<String, Path> filesMap = handler.task.getInputFilesMap()
        Set<Path> inputFiles = filesMap ? filesMap.values() as Set<Path> : new HashSet<Path>()
        storage.addInputs( handler.task, inputFiles )
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        Map<OutParam, ?> outputs = handler.task.getOutputs()
        Set<Path> outputFiles = new HashSet<>()
        for (final def it in outputs.entrySet()) {
            if (it.key instanceof FileOutParam) {
                if (it.value instanceof Path) {
                    outputFiles.add(it.value as Path)
                } else if (it.value instanceof Collection) {
                    outputFiles.addAll(it.value as Collection<Path>)
                }
            }
        }
        storage.addOutputs(handler.task, outputFiles)
    }

}
