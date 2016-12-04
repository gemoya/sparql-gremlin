/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastax.sparql;

import com.datastax.sparql.gremlin.SparqlToGremlinCompiler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
class ConsoleCompiler {

    public static void main(final String[] args) throws IOException {

        // opciones para los datos de entrada y creacion de grafo
        final Options options = new Options();
        options.addOption("f", "file", true, "a file that contains a SPARQL query");
        options.addOption("g", "graph", true, "the graph that's used to execute the query [classic|modern|crew|kryo file]");
        // TODO: add an OLAP option (perhaps: "--olap spark"?)
        System.out.println("tomando file como input\n");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine;
        

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            printHelp(1);
            return;
        }

        final InputStream inputStream = commandLine.hasOption("file")
                ? new FileInputStream(commandLine.getOptionValue("file"))
                : System.in;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        final StringBuilder queryBuilder = new StringBuilder();
        // segunda query
        final StringBuilder queryBuilder2 = new StringBuilder();

        if (!reader.ready()) {
            printHelp(1);
        }

        System.out.println("leyendo archivo\n");
        String line;
        String un1 = "UNION";
        String un2 = "union";
        // si hay union entonces no seguir con la query
        while (null != (line = reader.readLine())) {
            if (line.contains(un1) || line.contains(un2)){
                System.out.println("hay union");
                
                //borramos ultimo }
                line = line.substring(0,line.length() - 2);
                //borramos UNION {
                queryBuilder2.append(System.lineSeparator()).append(line.replace("UNION {", ""));
            } else { 
                queryBuilder.append(System.lineSeparator()).append(line);
                //System.out.println("queryBuilder en el while: " + queryBuilder.toString());
            }
            
        }

        
        final String queryString = queryBuilder.toString();
        final Graph graph;
        // segundo queryString y graph
        final String queryString2 = queryBuilder2.toString();
        
        // System.out.println("queryString: " + queryString);
        // System.out.println("queryString2: " + queryString2);
        if (commandLine.hasOption("graph")) {
            switch (commandLine.getOptionValue("graph").toLowerCase()) {
                case "classic": 
                   graph = TinkerFactory.createClassic();
                    break;
                case "modern":
                    graph = TinkerFactory.createModern();
                    break;
                case "crew":
                    graph = TinkerFactory.createTheCrew();
                    break;
                default:
                    graph = TinkerGraph.open();
                    graph.io(IoCore.gryo()).readGraph(commandLine.getOptionValue("graph"));
                    break;
            }
        } else {
            graph = TinkerFactory.createModern();
        }
        
        System.out.println("Construyendo traversal\n");
        final Traversal<Vertex, ?> traversal = SparqlToGremlinCompiler.convertToGremlinTraversal(graph, queryString);
        
        printWithHeadline("SPARQL Query", queryString);
        printWithHeadline("Traversal (prior execution)", traversal);
        printWithHeadline("Result", String.join(System.lineSeparator(),
                traversal.toStream().map(Object::toString).collect(Collectors.toList())));
        printWithHeadline("Traversal (after execution)", traversal);
        
        System.out.println("Segundo Traversal");
        final Traversal<Vertex, ?> traversal2 = SparqlToGremlinCompiler.convertToGremlinTraversal(graph, queryString2);
        printWithHeadline("SPARQL Query", queryString2);
        printWithHeadline("Traversal (prior execution)", traversal2);
        printWithHeadline("Result", String.join(System.lineSeparator(),
                traversal2.toStream().map(Object::toString).collect(Collectors.toList())));
        printWithHeadline("Traversal (after execution)", traversal2);
        
        
        
    }

    // funcion de 
    private static void printHelp(final int exitCode) throws IOException {
        final Map<String, String> env = System.getenv();
        final String command = env.containsKey("LAST_COMMAND") ? env.get("LAST_COMMAND") : "sparql-gremlin.sh";
        printWithHeadline("Usage Examples", String.join("\n",
                command + " -f examples/modern1.sparql",
                command + " < examples/modern2.sparql",
                command + " <<< 'SELECT * WHERE { ?a e:knows ?b }'",
                command + " -g crew < examples/crew1.sparql"));
        if (exitCode >= 0) {
            System.exit(exitCode);
        }
    }

    private static void printWithHeadline(final String headline, final Object content) throws IOException {
        final StringReader sr = new StringReader(content != null ? content.toString() : "null");
        final BufferedReader br = new BufferedReader(sr);
        String line;
        System.out.println();
        System.out.println("\u001B[1m" + headline + "\u001B[0m");
        System.out.println();
        boolean skip = true;
        while (null != (line = br.readLine())) {
            skip &= line.isEmpty();
            if (!skip) {
                System.out.println("  " + line);
            }
        }
        System.out.println();
        br.close();
        sr.close();
    }
}
