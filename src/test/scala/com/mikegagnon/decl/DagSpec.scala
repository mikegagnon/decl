/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.mikegagnon.decl

import scala.util.Random

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

object DagSpec {

  // returns true iff nodes is sorted
  def sorted[Node](nodes: Seq[Node], dag: Dag[Node]): Boolean = {

    assert(nodes.toSet == dag.nodes)

    val (_, isSorted) = nodes.foldLeft((Set[Node](), true)) {
        (visitedIsSorted: (Set[Node], Boolean), node: Node) =>
      val (visited, isSorted) = visitedIsSorted

      // the set of nodes that point to this node, but have not yet been visited
      val missingDependencies = dag.incoming(node) -- visited

      if (!isSorted || missingDependencies.nonEmpty) {
        (Set[Node](), false)
      } else {
        (visited + node, true)
      }
    }

    isSorted
  }
}

/**
 * i points to j iff matrix[i][j] == 1
 */
case class MatrixDag(matrix : Array[Array[Int]]) extends Dag[Int] {

  val numNodes = matrix(0).size
  assert(numNodes == 0 || matrix.size == matrix(0).size)

  val nodes = (0 until numNodes).toSet

  val edges = for {
    fromNode <- nodes
    toNode <- nodes if matrix(fromNode)(toNode) == 1
  } yield (fromNode, toNode)

}

@RunWith(classOf[JUnitRunner])
class DagSpec extends FlatSpec with ShouldMatchers {

  def checkSorted[T](dag : Dag[T]) = DagSpec.sorted(dag.sorted, dag) should equal (true)

  val matrixEmpty = Array(Array[Int]())
  val matrix1disjoint = Array(Array(0))
  val matrix1cycle = Array(Array(1))

  val matrix5fulldisjoint = Array(
    Array(0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0))

  val matrix5cycle = Array(
    Array(1, 1, 0, 1, 0),
    Array(0, 0, 1, 1, 0),
    Array(0, 1, 0, 0, 1),
    Array(1, 0, 0, 0, 1),
    Array(0, 0, 1, 0, 0))

  val matrix5 = Array(
    Array(0, 1, 0, 1, 0),
    Array(0, 0, 1, 1, 0),
    Array(0, 0, 0, 0, 1),
    Array(0, 0, 0, 0, 1),
    Array(0, 0, 0, 0, 0))

  val matrix5cycle2 = Array(
    Array(0, 1, 0, 1, 0),
    Array(0, 0, 1, 1, 0),
    Array(0, 0, 0, 0, 1),
    Array(0, 0, 0, 0, 1),
    Array(0, 0, 1, 0, 0))

    // a partitioned graph
  val matrix10 = Array(
    Array(0, 1, 0, 1, 0, 0, 0, 0, 0, 0), // part1
    Array(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), // -
    Array(0, 0, 0, 1, 0, 0, 0, 0, 0, 0), // -
    Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // -
    Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // part2
    Array(0, 0, 0, 0, 1, 0, 0, 0, 0, 0), // -
    Array(0, 0, 0, 0, 0, 1, 0, 1, 0, 0), // -
    Array(0, 0, 0, 0, 1, 1, 0, 0, 0, 0), // -
    Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 1), // part3
    Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)) // -

  "DagSpec.sorted" should "identify sorted nodes" in {
    DagSpec.sorted(Seq[Int](), MatrixDag(matrixEmpty)) should equal (true)
  }

  it should "identify sorted nodes for a graph with a single, disjoint node" in {
    val graph = MatrixDag(matrix1disjoint)
    DagSpec.sorted(Seq(0), graph) should equal (true)
  }

  it should "identify sorted nodes for fully disjoint nodes" in {
    val graph = MatrixDag(matrix5fulldisjoint)
    DagSpec.sorted(Seq(0,1,2,3,4), graph) should equal (true)
    DagSpec.sorted(Seq(4,3,2,1,0), graph) should equal (true)
  }

  it should "identify sorted nodes for a graph with 5 nodes" in {
    implicit val graph = MatrixDag(matrix5)
    DagSpec.sorted(Seq(0,1,2,3,4), graph) should equal (true)
    DagSpec.sorted(Seq(0,2,1,3,4), graph) should equal (false)
    DagSpec.sorted(Seq(0,1,3,2,4), graph) should equal (true)
    DagSpec.sorted(Seq(0,1,2,4,3), graph) should equal (false)
  }

  it should "identify sorted nodes for 9-node partitioned graph" in {

   /**
     * generate a random list of nodes that is topologically sorted.
     * parts is a list a of isolated partitions; each part is topologically sorted w.r.t. itself
     * generates a sorted list of nodes, repeatedly by popping the head off a random partition
     * and adding to the list
     */
    def randSorted(parts : List[List[Int]], soFar : List[Int] = Nil)
        (implicit rand : Random): List[Int] = {
      if (parts.isEmpty) {
        soFar.reverse
      } else {
        val (part :: otherParts) = rand.shuffle(parts)
        part match {
          case Nil => randSorted(otherParts, soFar)
          case node :: tail => randSorted(tail :: otherParts, node :: soFar)
        }
      }
    }

    /**
     * generate a random list of nodes that is ALMOST, BUT NOT QUITE, topologically sorted.
     * parts is a list a of isolated partitions; each part is topologically sorted w.r.t. itself
     *
     * achieves this goal by picking a random part, swapping two elements in that part, then
     * calling randSorted
     * NOTE: for this to work, each part may have only one valid sorting; otherwise swapping two
     * elements could lead to a valid sorting.
     */
    def randUnsorted(parts : List[List[Int]])(implicit rand : Random) : List[Int] = {

      // randomly choose the part to mutate
      val (part :: goodParts) = rand.shuffle(parts)

      // split the part in two halves (where each half has at least one element)
      val splitIndex = rand.nextInt(part.size - 1) + 1
      assert(splitIndex >= 1 && splitIndex <= part.size - 1)
      val (partA, partB) = part.splitAt(splitIndex)

      val nodeA = partA.last
      val beginA = partA.init
      val nodeB :: endB = partB

      val badPart = beginA ++ List(nodeB, nodeA) ++ endB

      assert(badPart.toSet == part.toSet)

      val badParts = badPart :: goodParts

      randSorted(badParts)
    }

    implicit val graph = MatrixDag(matrix10)

    val partA = List(0,1,2,3)
    val partB = List(6,7,5,4)
    val partC = List(8,9)
    val parts = List(partA, partB, partC)

    implicit val rand = new Random()

    (0 until 100).foreach { _ =>
      DagSpec.sorted(randSorted(parts), graph) should equal (true)
    }

    (0 until 100).foreach { _ =>
      DagSpec.sorted(randUnsorted(parts), graph) should equal (false)
    }

  }

  "graph.outgoing and graph.incoming" should "work for 5-node graph" in {
    val graph = MatrixDag(matrix5)

    evaluating { graph.outgoing(-1) } should produce [IllegalArgumentException]
    evaluating { graph.outgoing(5) } should produce [IllegalArgumentException]

    graph.outgoing(0) should equal (Set(1,3))
    graph.outgoing(1) should equal (Set(2,3))
    graph.outgoing(2) should equal (Set(4))
    graph.outgoing(3) should equal (Set(4))
    graph.outgoing(4) should equal (Set())

    graph.incoming(0) should equal (Set())
    graph.incoming(1) should equal (Set(0))
    graph.incoming(2) should equal (Set(1))
    graph.incoming(3) should equal (Set(0,1))
    graph.incoming(4) should equal (Set(2,3))
  }

  it should "work for 9-node partitioned graph" in {
    val graph = MatrixDag(matrix10)

    graph.outgoing(0) should equal (Set(1,3))
    graph.outgoing(1) should equal (Set(2))
    graph.outgoing(2) should equal (Set(3))
    graph.outgoing(3) should equal (Set())
    graph.outgoing(4) should equal (Set())
    graph.outgoing(5) should equal (Set(4))
    graph.outgoing(6) should equal (Set(5, 7))
    graph.outgoing(7) should equal (Set(4, 5))
    graph.outgoing(8) should equal (Set(9))
    graph.outgoing(9) should equal (Set())

    graph.incoming(0) should equal (Set())
    graph.incoming(1) should equal (Set(0))
    graph.incoming(2) should equal (Set(1))
    graph.incoming(3) should equal (Set(0,2))
    graph.incoming(4) should equal (Set(5,7))
    graph.incoming(5) should equal (Set(6,7))
    graph.incoming(6) should equal (Set())
    graph.incoming(7) should equal (Set(6))
    graph.incoming(8) should equal (Set())
    graph.incoming(9) should equal (Set(8))
  }

  "graph.ancestors" should "work for a graph with a single, disjoint node" in {
    MatrixDag(matrix1disjoint).ancestors(0) should equal (Set())
  }

  it should "work for a well-formed graph with 5 nodes" in {
    val graph = MatrixDag(matrix5)
    graph.ancestors(0) should equal (Set())
    graph.ancestors(1) should equal (Set(0))
    graph.ancestors(2) should equal (Set(0,1))
    graph.ancestors(3) should equal (Set(0,1))
    graph.ancestors(4) should equal (Set(0,1,2,3))
  }

  "graph.sort" should "work for a graph with zero nodes" in {
    checkSorted(MatrixDag(matrixEmpty))
  }

  it should "throw and exception for a graph with a single, looped-back node" in {
    evaluating { MatrixDag(matrix1cycle).sorted } should produce [CycleException]
  }

  it should "work for a graph with a single, disjoint node" in {
    checkSorted(MatrixDag(matrix1disjoint))
  }

  it should "throw an exception for a cycle graph with 5 nodes" in {
    evaluating { MatrixDag(matrix5cycle).sorted } should produce [CycleException]
    evaluating { MatrixDag(matrix5cycle2).sorted } should produce [CycleException]
  }

  it should "work for a well-formed graph with 5 nodes" in {
    checkSorted(MatrixDag(matrix5))
  }

  it should "work for 10-node partitioned graph" in {
    checkSorted(MatrixDag(matrix10))
  }

  "graph.subgraph" should "work for a graph with zero nodes" in {
    val subgraph = MatrixDag(matrixEmpty).subgraph(Set[Int]())
    subgraph.nodes should equal (Set[Int]())
    subgraph.sorted should equal (List[Int]())
    subgraph.incoming should equal (Map[Int, Set[Int]]())
    subgraph.outgoing should equal (Map[Int, Set[Int]]())
  }

  it should "work for a graph with a single, disjoint node" in {
    val subgraph = MatrixDag(matrix1disjoint).subgraph(Set(0))
    subgraph.nodes should equal (Set(0))
    subgraph.sorted should equal (List(0))
    subgraph.incoming should equal (Map[Int, Set[Int]]())
    subgraph.outgoing should equal (Map[Int, Set[Int]]())
  }

  it should "for a fully connected graph with 5 nodes for subgraph(0)" in {
    val subgraph = MatrixDag(matrix5).subgraph(Set(0))
    subgraph.nodes should equal (Set(0))
    subgraph.sorted should equal (List(0))
    subgraph.incoming should equal (Map[Int, Set[Int]]())
    subgraph.outgoing should equal (Map[Int, Set[Int]]())
  }

  it should "for a fully connected graph with 5 nodes for subgraph(1)" in {
    val subgraph = MatrixDag(matrix5).subgraph(Set(1))
    subgraph.nodes should equal (Set(0,1))
    subgraph.sorted should equal (List(0,1))
    subgraph.outgoing should equal (Map(0->Set(1)))
    subgraph.incoming should equal (Map(1->Set(0)))
  }

  it should "for a fully connected graph with 5 nodes for subgraph(2)" in {
    val subgraph = MatrixDag(matrix5).subgraph(Set(2))
    subgraph.nodes should equal (Set(0,1,2))
    subgraph.sorted should equal (List(0,1,2))
    subgraph.outgoing should equal (Map(0->Set(1),1->Set(2)))
    subgraph.incoming should equal (Map(1->Set(0),2->Set(1)))
  }

  it should "for a fully connected graph with 5 nodes for subgraph(3)" in {
    val subgraph = MatrixDag(matrix5).subgraph(Set(3))
    subgraph.nodes should equal (Set(0,1,3))
    subgraph.sorted should equal (List(0,1,3))
    subgraph.outgoing should equal (Map(0->Set(1, 3),1->Set(3)))
    subgraph.incoming should equal (Map(1->Set(0),3->Set(0,1)))
  }

  it should "for a fully connected graph with 5 nodes for subgraph(2,3)" in {
    val subgraph = MatrixDag(matrix5).subgraph(Set(2,3))
    subgraph.nodes should equal (Set(0,1,2,3))
    Set(List(0,1,2,3), List(0,1,3,2)) should contain (subgraph.sorted)
    subgraph.outgoing should equal (Map(0->Set(1,3),1->Set(2,3)))
    subgraph.incoming should equal (Map(1->Set(0),2->Set(1),3->Set(0,1)))
  }
}
