package nextflow.dataflow.helper

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.util.MemoryUnit

@CompileStatic
@Slf4j
class StringHelper {

    static String formatSize(long size) {
        return new MemoryUnit(size).toString().replace( " ", "" )
    }

    static String formatFiles(int files) {
        return files == 1 ? "1 file" : "$files files"
    }

    static boolean stringToBoolean(Object value, boolean defaultValue) {
        if (value instanceof Boolean) return value
        String strValue = value?.toString()
        if (strValue == null) return defaultValue
        switch ( strValue.toLowerCase() ) {
            case 'true':
            case 'yes':
            case '1':
                return true
            case 'false':
            case 'no':
            case '0':
                return false
            default:
                log.warn "Invalid boolean value: $strValue -- using default value: $defaultValue"
                return defaultValue
        }


    }

}
