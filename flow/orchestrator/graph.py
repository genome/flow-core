#!/usr/bin/env python

from pygraphviz import AGraph

def transitive_reduction(edges):
    """Perform transitive reduction on the (directed) graph edges."""
    graph = AGraph(directed=True)

    # Graphviz converts node names to unicode. To avoid changing the types
    # our edge names, we create a mapping to translate back.
    node2gv = {} # node name to graphviz name
    gv2node = {} # graphviz name to node name
    for src, dst_set in edges.iteritems():
        gv_src = node2gv.setdefault(src, unicode(len(node2gv)))
        gv2node[gv_src] = src
        for dst in dst_set:
            gv_dst = node2gv.setdefault(dst, unicode(len(node2gv)))
            gv2node[gv_dst] = dst
            graph.add_edge(node2gv[src], node2gv[dst])

    graph.tred()

    reduced_edges = {}
    for gv_src, gv_dst in graph.edges():
        src = gv2node[gv_src]
        dst = gv2node[gv_dst]
        reduced_edges.setdefault(src, set([])).add(dst)

    return reduced_edges
