package io.disys.axis.storage.swmr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BoundedBTreeIterator<K, V> implements Iterator<KeyVal<K, V>> {

    private final List<Frame<K, V>> path;
    private Node.Leaf<K, V> currentLeaf;
    private int currentIdx;
    private int endIdx;
    private final K to;

    private static class Frame<K, V> {
        final Node.Internal<K, V> node;
        int idx;

        private Frame(Node.Internal<K, V> node, int idx) {
            this.node = node;
            this.idx = idx;
        }

        Node<K, V> next() {
            if (idx >= node.children().size()) {
                return null;
            }

            return node.children().child(idx++);
        }
    }

    private BoundedBTreeIterator(List<Frame<K, V>> path, Node.Leaf<K, V> leaf, int leafStart, int leafEnd, K to) {
        this.path = path;
        this.currentLeaf = leaf;
        this.currentIdx = leafStart;
        this.endIdx = leafEnd;
        this.to = to;
    }


    static <K, V> BoundedBTreeIterator<K, V> of(Node<K, V> node, K from, K to) {
        var path = new ArrayList<Frame<K, V>>();
        while (node instanceof Node.Internal<K,V> next) {
            var lb = Search.lowerBound(next.keys(), from);
            var frame = new Frame<>(next, lb.idx());
            path.add(frame);
            node = frame.next();
        }

        assert node instanceof Node.Leaf<K, V>;
        var leaf = (Node.Leaf<K, V>) node;

        var start = Search.lowerBound(leaf.keys(), from).idx();
        var endLb = Search.lowerBound(leaf.keys(), to);
        var end = endLb.found() ? endLb.idx() : endLb.idx() - 1;

        return new BoundedBTreeIterator<>(path, leaf, start, end, to);
    }


    @Override
    public boolean hasNext() {
        if (currentIdx <= endIdx) {
            return true;
        }

        if (path.isEmpty()) {
            return false;
        }

        var node = path.getLast().next();
        while (!path.isEmpty() && node == null) {
            path.removeLast();
            if (path.isEmpty()) {
                break;
            }
            node = path.getLast().next();
        }

        if (path.isEmpty()) {
            return false;
        }

        while (node instanceof Node.Internal<K,V> nextNode) {
            var frame = new Frame<>(nextNode, 0);
            path.add(frame);
            node = frame.next();
        }

        assert node instanceof Node.Leaf<K, V>;
        var nextLeaf = (Node.Leaf<K, V>) node;

        var endLb = Search.lowerBound(nextLeaf.keys(), to);
        var end = endLb.found() ? endLb.idx() : endLb.idx() - 1;

        if (end < 0) {
            path.clear();
            return false;
        }

        currentLeaf = nextLeaf;
        currentIdx = 0;
        endIdx = end;

        return true;
    }

    @Override
    public KeyVal<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var idx = currentIdx++;
        return new KeyVal<>(currentLeaf.keys().key(idx), currentLeaf.values().val(idx));
    }
}
