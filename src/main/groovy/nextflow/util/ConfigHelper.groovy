/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.util

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import org.codehaus.groovy.runtime.InvokerHelper
/**
 * Helper method to handle configuration object
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class ConfigHelper {


    def static getConfigProperty( def config, String execName, String propName ) {
        def result = null

        // make sure that the *executor* is a map object
        // it could also be a plain string (when it specifies just the its name)
        if( execName && config instanceof Map && config['$'+execName] instanceof Map ) {
            result = config['$'+execName][propName]
        }

        if( result==null && config instanceof Map && config[propName] ) {
            result = config[propName]
        }

        return result
    }

    /**
     * Given a string value converts to its native object representation.
     *
     * @param str A string that may represent boolean, integer, {@link Duration} values
     * @return A object representing the argument value of the string itself if it was not a boolean/number/duration value
     */
    static parseValue( String str ) {

        if ( str == null ) return null

        if ( str.toLowerCase() == 'true') return Boolean.TRUE
        if ( str.toLowerCase() == 'false' ) return Boolean.FALSE

        if ( str.isInteger() ) return str.toInteger()
        if ( str.isLong() ) return str.toLong()
        if ( str.isDouble() ) return str.toDouble()
        // try as duration as well
        try { return new Duration(str) }
        catch( IllegalArgumentException e ) { }

        return str

    }

    static parseValue( obj ) {
        if( obj instanceof String )
            return parseValue((String)obj)

        if( obj instanceof GString )
            return parseValue(obj.toString())

        return obj
    }

    /**
     * Given a list of paths looks for the files ending withe the extension '.jar' and return
     * a list containing the original directories, plus the JARs paths
     *
     * @param dirs
     * @return
     */
    static List<Path> resolveClassPaths( List<Path> dirs ) {

        List<Path> result = []
        if( !dirs )
            return result

        for( Path path : dirs ) {
            if( path.isFile() && path.name.endsWith('.jar') ) {
                result << path
            }
            else if( path.isDirectory() ) {
                result << path
                path.eachFileMatch( ~/.+\.jar$/ ) { if(it.isFile()) result << it }
            }
        }

        return result
    }

    static private final String TAB = '   '

    static private void canonicalFormat(StringBuilder writer, ConfigObject object, int level, boolean sort) {

        final keys = sort ? object.keySet().sort() : new ArrayList<>(object.keySet())

        // remove all empty config objects
        final itr = keys.iterator()
        while( itr.hasNext() ) {
            final key = itr.next()
            final value = object.get(key)
            if( value instanceof ConfigObject && value.size()==0 ) {
                itr.remove()
            }
        }

        for( int i=0; i<keys.size(); i++) {
            final key = keys[i]
            final value = object.get(key)
            if( value instanceof ConfigObject ) {
                // add an extra new-line to separate simple values from a config object
                if( level==0 && i>0 ) {
                    writer.append('\n')
                }

                writer.append(TAB*level)
                writer.append(wrap1(key))
                writer.append(' {\n')
                canonicalFormat(writer, value, level+1,sort)
                writer.append(TAB*level)
                writer.append('}\n')
            }
            else {
                // add a new-line to separate simple values from a previous config object
                if( level==0 && i>0 && object.get(keys[i-1]) instanceof ConfigObject) {
                    writer.append('\n')
                }

                writer.append(TAB*level)
                writer.append(wrap1(key))
                writer.append(' = ')
                writer.append( render0(value) )
                writer.append('\n')
            }
        }
    }

    static @PackageScope String wrap1(param) {
        final key = param.toString()
        if( key.startsWith('withLabel:') )  {
            return 'withLabel:' + wrap0(key.substring('withLabel:'.length()))
        }
        else if( key.startsWith('withName:') )  {
            return 'withName:' + wrap0(key.substring('withName:'.length()))
        }
        else {
            return wrap0(key)
        }
    }

    static @PackageScope String wrap0( param ) {
        def key = param.toString()
        isValidIdentifier(key) ? key : "'$key'"
    }

    static private String propertiesFormat(Properties properties) {
        def buffer = new ByteArrayOutputStream()
        properties.store(buffer,null)
        buffer.flush()

        def result = new StringBuilder()
        for( String line : buffer.toString().readLines() ) {
            if(line.startsWith('#')) continue
            result << line << '\n'
        }
        result.toString()
    }

    static private String flattenFormat(ConfigObject config,boolean sort) {
        def result = new StringBuilder()
        flattenFormat(config, [], result, sort)
        result.toString()
    }

    static private void flattenFormat(ConfigObject config, List<String> stack, StringBuilder result, boolean sort) {
        final keys = sort ? config.keySet().sort() : new ArrayList<>(config.keySet())

        for( int i=0; i<keys.size(); i++) {
            final key = keys.get(i)
            final val = config.get(key)
            stack.add(wrap0(key))
            if( val instanceof ConfigObject ) {
                flattenFormat(val, stack, result, sort)
            }
            else {
                final name = stack.join('.')
                result << name << ' = ' << render0(val) << '\n'
            }
            stack.removeLast()
        }

    }

    private static String render0( val ) {
        if( val == null )
            return 'null'
        if( val instanceof GString )
            return "'$val'"
        if( val instanceof MemoryUnit )
            return "'$val'"
        if( val instanceof Duration )
            return "'$val'"

        InvokerHelper.inspect(val)
    }

    static String toCanonicalString(ConfigObject object, boolean sort=false) {
        def result = new StringBuilder()
        canonicalFormat(result,object,0,sort)
        result.toString()
    }

    static String toCanonicalString(Map map, boolean sort=false) {
        toCanonicalString(map.toConfigObject(), sort)
    }

    static String toPropertiesString(ConfigObject config, boolean sort=false) {
        def result = propertiesFormat(config.toProperties())
        if( !result ) return result
        sort ? result.readLines().sort().join('\n')+'\n' : result
    }

    static String toPropertiesString(Map map, boolean sort=false) {
        toPropertiesString(map.toConfigObject(), sort)
    }

    static String toFlattenString(ConfigObject object, boolean sort=false) {
        flattenFormat(object, sort)
    }

    static String toFlattenString(Map map, boolean sort=false) {
        flattenFormat(map.toConfigObject(), sort)
    }

    static boolean isValidIdentifier(String s) {
        // an empty or null string cannot be a valid identifier
        if (s == null || s.length() == 0) {
            return false;
        }

        char[] c = s.toCharArray();
        if (!Character.isJavaIdentifierStart(c[0])) {
            return false;
        }

        for (int i = 1; i < c.length; i++) {
            if (!Character.isJavaIdentifierPart(c[i])) {
                return false;
            }
        }

        return true;
    }



}

