package com.mikegagnon.decl

class CycleException(msg: String) extends IllegalArgumentException(msg)

/**
 * Directed Acyclic Graph
 *
 * Subclasses should override:
 *    - nodes
 *    - edges
 * Provides the following public methods/fields:
 *    - sorted
 *    - ancestors
 *    - subgraph
 */
abstract class Dag[Node] {

  // the set of all nodes in this graph
  val nodes: Set[Node]

  // set of (from, to) edges
  val edges: Set[(Node, Node)]

  // given a set of (from, to) pairs, return a map that maps each node to the set of nodes that it
  // points to
  private[decl] def edgesToMap(fromTo: Set[(Node, Node)]): Map[Node, Set[Node]] = fromTo
    // yields a map, which maps each "to node" to its list of (from, to) pairs
    .groupBy { case (from, _) => from }
    // drop the "to node" from the list of pairs
    .mapValues { pairs: Set[(Node, Node)] =>
      pairs.map { fromTo: (Node, Node) =>
        fromTo._2
      }
    }
    .withDefault{ node =>
      if (nodes.contains(node)) {
        Set[Node]()
      } else {
        throw new IllegalArgumentException("nodes %s is not in the set of nodes".format(node))
      }
    }

  // maps each node to the set of nodes it points to
  private[decl] final lazy val outgoing: Map[Node, Set[Node]] = edgesToMap(edges)

  // maps each node to the set of nodes that point to it
  private[decl] final lazy val incoming: Map[Node, Set[Node]] =
    edgesToMap(edges.map{ case (from, to) => (to, from) })

  // set of all root nodes
  private[decl] final lazy val roots: Set[Node] = nodes.
    filter { node =>
      incoming(node).isEmpty
    }

  // the sorted list of nodes
  final lazy val sorted: List[Node] = {

    /**
     * Algorithm from Wikipedia:
     *
     * soFar = Empty list that will contain the sorted nodes
     * roots = Set of all nodes with no incoming edges (the beginning)
     * for each node n in roots do
     *      visit(n, stack=emptySet)
     *  function visit(node n, stack)
     *      if n in stack then CycleDetected
     *      if n has not been visited yet then
     *          mark n as visited
     *          for each node m with an edge from n to m do
     *              visit(m, stack + n)
     *          prepend n to L
     */

    if (roots.isEmpty && nodes.nonEmpty) {
      throw new CycleException("There are no root nodes")
    }

    val (sortedNodes, _) = roots.foldLeft((List[Node](), Set[Node]())) {
        case ((soFar, visited), rootNode) =>
      visit(rootNode, soFar, visited, Set())
    }

    assert(sortedNodes.toSet == nodes)

    sortedNodes
  }

  // returns all ancestors for a particular node
  final def ancestors(node: Node, stack: Set[Node] = Set[Node]()): Set[Node] = {
    if (stack.contains(node)) {
      throw new CycleException("Cycled detected at node %s".format(node))
    }

    val in = incoming(node)
    val newStack = stack + node
    in ++ in.flatMap{ parentNode => ancestors(parentNode, newStack) }
  }

  /**
   * Returns the subgraph that contains leaves plus all of leaves's ancestors.
   */
  final def subgraph(leaves: Set[Node]): Dag[Node] = {
    val subnodes = leaves ++ leaves.flatMap{ ancestors(_) }
    val subedges = edges.filter{ case (from, to) =>
        subnodes.contains(from) && subnodes.contains(to)
      }
    new Dag[Node] {
      val nodes = subnodes
      val edges = subedges
    }
  }

  /**
   * For explanation of this algorithm see comment for the sorted value
   *
   * node: the node currently being visited
   * soFar: the sorted list of nodes, so far. This is a depth first search, so the last nodes are
   *    inserted first and nodes are incrementally prepended.
   * visited: the list of nodes that visit has been called upon thus far
   * stack: all the nodes in the current visit call stack (used for detecting cycles)
   */
  private[decl] final def visit(node: Node, soFar: List[Node], visited: Set[Node],
      stack: Set[Node]): (List[Node], Set[Node]) = {

    if (stack.contains(node)) {
      throw new CycleException("Cycled detected at node %s".format(node))
    }

    val newStack = stack + node

    if (!visited.contains(node)) {
      val newSoFarVisited: (List[Node], Set[Node]) = outgoing(node)
        .foldLeft((soFar, visited + node)) {
            (soFarVisited: (List[Node], Set[Node]), childNode: Node) =>
          val soFar: List[Node] = soFarVisited._1
          val visited: Set[Node] = soFarVisited._2
          visit(childNode, soFar, visited, newStack)
        }
      val newSoFar: List[Node] = newSoFarVisited._1
      val newVisited: Set[Node] = newSoFarVisited._2
      (node :: newSoFar, newVisited)
    } else {
      (soFar, visited)
    }
  }
}
