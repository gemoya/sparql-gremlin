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

package com.datastax.sparql.gremlin;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.List;
import org.apache.jena.sparql.syntax.Element;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
// TODO: implement OpVisitor, don't extend OpVisitorBase
public class SparqlToGremlinCompiler extends OpVisitorBase {

    private GraphTraversal<Vertex, ?> traversal;

    private SparqlToGremlinCompiler(final GraphTraversal<Vertex, ?> traversal) {
        this.traversal = traversal;
    }

    private SparqlToGremlinCompiler(final GraphTraversalSource g) {
        this(g.V());
    }

    GraphTraversal<Vertex, ?> convertToGremlinTraversal(final Query query) {
        final Op op = Algebra.compile(query);
        // no puedo sobrecargar compile tal que separe los Op para pasar a walk
        // Opleft y Opright
        
        System.out.println(op.toString());
        //Element queryElement = query.getQueryPattern();
        //queryElement.
        System.out.println("compilando query");
        OpWalker.walk(op, this);
        if (!query.isQueryResultStar()) {
            System.out.println("Entrando al if");
            // creo que aqui solo extraen el nombre de las variables de la query
            final List<String> vars = query.getResultVars();
            switch (vars.size()) {
                case 0:
                    throw new IllegalStateException();
                case 1:
                    if (query.isDistinct()) {
                    	System.out.println("caso 1 var, antes del dedup");
                        traversal = traversal.dedup(vars.get(0));
                        System.out.println("caso 1 var, despues del dedup");
                    }
                    traversal = traversal.select(vars.get(0));
                    break;
                case 2:
                    if (query.isDistinct()) {
                        traversal = traversal.dedup(vars.get(0), vars.get(1));
                    }
                    traversal = traversal.select(vars.get(0), vars.get(1));
                    break;
                default:
                    System.out.println(vars);
                    // se convierten las variables a un array
                    final String[] all = new String[vars.size()];
                    vars.toArray(all);
                    System.out.println(all[0]);
                    if (query.isDistinct()) {
                        System.out.println("en el if del default:");
                        traversal = traversal.dedup(all);
                    }
                    final String[] others = Arrays.copyOfRange(all, 2, vars.size());
                    traversal = traversal.select(vars.get(0), vars.get(1), others);
                    break;
            }
        } else {
        	System.out.println("entrando al else");
            if (query.isDistinct()) {
                traversal = traversal.dedup();
            }
        }
        
        return traversal;
    }

    private static GraphTraversal<Vertex, ?> convertToGremlinTraversal(final GraphTraversalSource g, final Query query) {
        System.out.println("en el metodo que recibe GraphTraversalSource g y Query query");
        return new SparqlToGremlinCompiler(g).convertToGremlinTraversal(query);
    }

    public static GraphTraversal<Vertex, ?> convertToGremlinTraversal(final Graph graph, final String query) {
        System.out.println("en el metodo que recibe Graph graph y String query");
        Query query2  = QueryFactory.create(Prefixes.prepend(query));
        System.out.println(query2.toString());
    	return convertToGremlinTraversal(graph.traversal(), QueryFactory.create(Prefixes.prepend(query), Syntax.syntaxSPARQL));
    }

    public static GraphTraversal<Vertex, ?> convertToGremlinTraversal(final GraphTraversalSource g, final String query) {
        System.out.println("en el metodo que recibe GraphTraversal g y String query");
        return convertToGremlinTraversal(g, QueryFactory.create(Prefixes.prepend(query), Syntax.syntaxSPARQL));
    }

    @Override
    public void visit(final OpBGP opBGP) {
        System.out.println("en el visit 1");
        final List<Triple> triples = opBGP.getPattern().getList();
        System.out.println(opBGP.getPattern());
        final Traversal[] matchTraversals = new Traversal[triples.size()];
        int i = 0;
        System.out.println("Triples y por cada uno TraversalBuilder.transform(triple)");
        for (final Triple triple : triples) {
            System.out.println(triple);
            matchTraversals[i++] = TraversalBuilder.transform(triple);
        }
        traversal = traversal.match(matchTraversals);
    }

    @Override
    public void visit(final OpFilter opFilter) {
        System.out.println("en el visit 2 ");
        opFilter.getExprs().getList().stream().
                map(WhereTraversalBuilder::transform).
                 reduce(traversal, GraphTraversal::where);
    }
    
    // se pierde la nocion de las operaciones anteriores
    @Override
    public void visit(final OpUnion OpUnion){
        System.out.println("En UNION");
        System.out.println(OpUnion.toString());
        System.out.println(OpUnion.getRight().toString());
        System.out.println(OpUnion.getLeft().toString());
        
       
    }
    
    /*
    @Override
    public void visit(final OpLeftJoin OpLeftJoin) {
        System.out.println("En OPTIONAL");
    
    }
    
    
    /*
    
    create(Op left, Op right) Create a union, dropping any nulls.
    
    */
    
    
    
}
