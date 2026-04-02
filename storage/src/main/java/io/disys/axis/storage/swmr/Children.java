package io.disys.axis.storage.swmr;

import java.util.Arrays;

public class Children<K, V> {
    private final Node<K, V>[] nodes;

    private Children(Node<K, V>[] nodes) {
        this.nodes = nodes;
    }

    // children can only exist if there is a single key
    // invariant: children.length = keys.size() + 1
    // therefore, always children.length >= 2
    static <K, V> Children<K, V> of(Node<K, V> left, Node<K, V> right) {
        return new Children<K, V>(new Node[]{left, right})
    }

    int size() {
        return nodes.length;
    }

    Node<K, V> child(int idx) {
        checkBounds(idx);
        return nodes[idx];
    }

    Children<K, V> insert(int idx, Node<K, V> node) {
        checkBounds(idx);

        var newNodes = Arrays.copyOf(nodes, nodes.length);
        newNodes[idx] = node;
        return new Children<>(newNodes);
    }


    Children<K, V> insert(int idx, Node<K, V> left, Node<K, V> right) {
        checkInsertBounds(idx);

        var newNodes = (Node<K,V>[]) new Node[nodes.length + 1];

        System.arraycopy(nodes, 0, newNodes, 0, idx);
        newNodes[idx] = left;
        newNodes[idx + 1] = right;
        System.arraycopy(nodes, idx + 1, newNodes, idx + 2, nodes.length - idx - 1);

        return new Children<>(newNodes);
    }

    ChildrenSplit<K, V> insertAndSplit(int insertIdx, int splitIdx, Node<K, V> left, Node<K, V> right) {
        checkInsertBounds(insertIdx);
        checkSplitBounds(splitIdx);

        var insertIdxForRight = insertIdx + 1;
        if (insertIdxForRight >= splitIdx) {
            var leftNodes = (Node<K, V>[]) new Node[splitIdx];
            System.arraycopy(nodes, 0, leftNodes, 0, splitIdx);

            var rightNodes = (Node<K, V>[]) new Node[nodes.length - splitIdx + 1];
            var prefixLen = insertIdxForRight - splitIdx;
            var suffixLen = nodes.length - insertIdxForRight;

            System.arraycopy(nodes, splitIdx, rightNodes, 0, prefixLen);
            rightNodes[prefixLen] = right;
            System.arraycopy(nodes, insertIdxForRight, rightNodes, prefixLen + 1, suffixLen);

            // i.e. rightInsertIdx == splitIdx, therefore left is the last node of leftNodes
            if (prefixLen == 0) {
                leftNodes[leftNodes.length - 1] = left;
            } else {
                // left is part of rightNodes comes in last of the prefix length
                rightNodes[prefixLen - 1] = left;
            }

            return new ChildrenSplit<>(
                    new Children<>(leftNodes),
                    new Children<>(rightNodes)
            );
        }

        var leftNodes = (Node<K, V>[]) new Node[splitIdx + 1];
        System.arraycopy(nodes, 0, leftNodes, 0, insertIdxForRight);
        leftNodes[insertIdx] = left;
        leftNodes[insertIdxForRight] = right;
        System.arraycopy(nodes, insertIdxForRight, leftNodes, insertIdxForRight + 1, splitIdx - insertIdxForRight);

        var splitIdxAfterInsertion = splitIdx - 1;
        var rightNodes = (Node<K, V>[]) new Node[nodes.length - splitIdxAfterInsertion];
        System.arraycopy(nodes, splitIdxAfterInsertion, rightNodes, 0, rightNodes.length);

        return new ChildrenSplit<>(
                new Children<>(leftNodes),
                new Children<>(rightNodes)
        );
    }

    private void checkInsertBounds(int idx) {
        if (idx < 0 || idx >= nodes.length) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds for insert: " + "[" + 0 + " " + nodes.length + ")");
        }
    }

    private void checkSplitBounds(int idx) {
        if (idx <= 0 || idx > nodes.length) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds for split: " + "(" + 0 + " " + nodes.length + "]");
        }
    }

    private void checkBounds(int idx) {
        if (idx < 0 || idx > nodes.length) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds: " + "[" + 0 + " " + nodes.length + ")");
        }
    }


}
