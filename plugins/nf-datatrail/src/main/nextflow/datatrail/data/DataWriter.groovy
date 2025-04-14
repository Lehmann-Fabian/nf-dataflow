package nextflow.datatrail.data

import groovy.transform.CompileStatic

import java.nio.file.Path

@CompileStatic
class DataWriter {

    private final PrintWriter writer
    private final String delimiter

    DataWriter( Path path, String delimiter ) {
        this.delimiter = delimiter
        path.parent.mkdirs()
        writer = new PrintWriter(path.toFile())
        writer.println("name${delimiter}hash${delimiter}path${delimiter}type${delimiter}size")
    }

    private void writeLine( String name, String hash, String path, String type, long size ) {
        writer.println("$name$delimiter$hash$delimiter$path$delimiter$type$delimiter$size")
    }

    void addTask( String name, String hash, Collection<Path> data ) {
        synchronized (writer) {
            data.each { path ->
                def size = DatatrailStorage.calculateSize( path )
                writeLine(name, hash, path.toString(), path.isDirectory() ? 'd' : 'f', size)
            }
            writer.flush()
        }
    }

    void close() {
        writer.close()
    }

}
