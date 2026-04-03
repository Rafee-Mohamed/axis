package io.disys.axis.storage.swmr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BTreeIterator<K, V> implements Iterator<KeyVal<K, V>> {

    private final List<Frame<K, V>> path;
    private Node.Leaf<K, V> currentLeaf;
    private int currentIdx;

    private static class Frame<K, V> {
        final Node.Internal<K, V> node;
        int idx;

        private Frame(Node.Internal<K, V> node) {
            this.node = node;
            this.idx = 0;
        }

        Node<K, V> next() {
            if (idx >= node.children().size()) {
                return null;
            }

            return node.children().child(idx++);
        }
    }

    private BTreeIterator(List<Frame<K, V>> path, Node.Leaf<K, V> leaf) {
        this.path = path;
        this.currentLeaf = leaf;
        this.currentIdx = 0;
    }


    static <K, V> BTreeIterator<K, V> of(Node<K, V> node) {
        var path = new ArrayList<Frame<K, V>>();
        while (node instanceof Node.Internal<K,V> next) {
            var frame = new Frame<>(next);
            path.add(frame);
            node = frame.next();
        }

        assert node instanceof Node.Leaf<K, V>;
        var leaf = (Node.Leaf<K, V>) node;

        return new BTreeIterator<>(path, leaf);
    }


    @Override
    public boolean hasNext() {
        if (currentIdx < currentLeaf.keys().size()) {
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
            var frame = new Frame<>(nextNode);
            path.add(frame);
            node = frame.next();
        }

        assert node instanceof Node.Leaf<K, V>;
        currentLeaf = (Node.Leaf<K, V>) node;
        currentIdx = 0;

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
