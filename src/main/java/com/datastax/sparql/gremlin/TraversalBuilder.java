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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;


/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
class TraversalBuilder {
    // Este metodo transforma la consulta SPARQL utilizando la ontologia
    // vocabulario que se relaciona con los componentes del traversal en gremlin
    // por cada triple toma el predicado y este define si se atribuye a
    // un vertice o una arista del grafo tinkerpop
    public static GraphTraversal<?, ?> transform(final Triple triple) {
        //System.out.println("\nEn TraversalBuilder");
        final GraphTraversal<Vertex, ?> matchTraversal = __.as(triple.getSubject().getName());
        //System.out.println("Sujeto: " + triple.getSubject().getName());
        final Node predicate = triple.getPredicate();
        //System.out.println("Predicado: " + triple.getPredicate());
        final String uri = predicate.getURI();
        //System.out.println("URI: " + uri);
        final String uriValue = Prefixes.getURIValue(uri);
        //System.out.println("URIValue: " + uriValue);
        final String prefix = Prefixes.getPrefix(uri);
        //System.out.println("PREFIX: " + prefix);
        //System.out.println("Object: " + triple.getObject());
        switch (prefix) {
            case "edge":
                //System.out.println("case: prefix = " + prefix);
                //System.out.println("out("+uriValue+").as("+triple.getObject().getName()+")");
                // moverse a un vertex al cual se pueda llegar con el valor del edge como uriValue y se renombra como el objeto (variable)
                return matchTraversal.out(uriValue).as(triple.getObject().getName());
            case "property":
                //System.out.println("case: prefix = " + prefix);
                return matchProperty(matchTraversal, uriValue, PropertyType.PROPERTY, triple.getObject());
            case "value":
                //System.out.println("case: prefix = " + prefix);
                return matchProperty(matchTraversal, uriValue, PropertyType.VALUE, triple.getObject());
            default:
                throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
        }
    }

    private static GraphTraversal<?, ?> matchProperty(final GraphTraversal<?, ?> traversal, final String propertyName,
                                                      final PropertyType type, final Node object) {
        
        // para saber que significa concreto
        if(object.isConcrete()) {
            System.out.println("Es CONCRETO");
        } else {
            System.out.println("NO es concreto");
        }
        
        switch (propertyName) {
            case "id":
                return object.isConcrete()
                        ? traversal.hasId(object.getLiteralValue())
                        : traversal.id().as(object.getName());
            case "label":
                return object.isConcrete()
                        ? traversal.hasLabel(object.getLiteralValue().toString())
                        : traversal.label().as(object.getName());
            case "key":
                return object.isConcrete()
                        ? traversal.hasKey(object.getLiteralValue().toString())
                        : traversal.key().as(object.getName());
            case "value":
                return object.isConcrete()
                        ? traversal.hasValue(object.getLiteralValue().toString())
                        : traversal.value().as(object.getName());
            default:
                if (type.equals(PropertyType.PROPERTY)) {
                    return traversal.properties(propertyName).as(object.getName());
                } else {
                    return object.isConcrete()
                            ? traversal.values(propertyName).is(object.getLiteralValue())
                            : traversal.values(propertyName).as(object.getName());
                }
        }
    }
}
