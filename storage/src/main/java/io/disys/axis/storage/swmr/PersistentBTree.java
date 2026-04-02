package io.disys.axis.storage.swmr;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

// Single Writer Multi Reader ordered index
public class PersistentBTree<K, V> {
    private volatile Node<K, V> root;
    private final int maxKeys;
    private final int splitAt;
    private final KeyStorageFactory<K> ksf;

    PersistentBTree(int maxKeys, KeyStorageFactory<K> ksf) {
        this.maxKeys = maxKeys;
        this.splitAt = maxKeys / 2;
        this.ksf = ksf;
    }

    void put(K key, V val) {
        root = switch (put(root, key, val)) {
            case PutResult.NoSplit<K, V>(var node) -> node;
            case PutResult.Split<K, V>(var left, var right, var promotedKey) ->
                    new Node.Internal<>(ksf.single(promotedKey), Children.of(left, right));
        };
    }

    record LowerBound(boolean found, int idx) {}

    LowerBound lowerBound(KeyStorage<K> keys, K key) {
        var left = 0;
        var right = keys.size() - 1;

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var cmp = keys.compare(mid, key);

            if (cmp == 0) {
                return new LowerBound(true, mid);
            }

            if (cmp > 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return new LowerBound(false, left);
    }

    // ====== GET ======

    V get(K key) {
        var node = root;
        if (node == null) {
            return null;
        }

        return get(node, key);
    }

    V get(Node<K, V> node, K key) {
        return switch (node) {
            case Node.Internal<K, V>(var keys, var children) -> {
                var lb = lowerBound(keys, key);
                var childIdx = lb.found() ? lb.idx() + 1 : lb.idx();
                yield get(children.child(childIdx), key);
            }

            case Node.Leaf<K, V>(var keys, var vals) -> {
                var lb = lowerBound(keys, key);
                yield lb.found() ? vals.val(lb.idx()) : null;
            }
        };
    }


    // ====== PUT ======

    PutResult<K, V> put(Node<K, V> node, K key, V val) {
        if (node == null) {
            return new PutResult.NoSplit<>(new Node.Leaf<>(
                    ksf.single(key), ValueStorage.of(val)
            ));
        }

        return switch (node) {
            case Node.Internal<K, V>(var keys, var children) -> putInternal(keys, children, key, val);
            case Node.Leaf<K, V>(var keys, var vals) -> putLeaf(keys, vals, key, val);
        };
    }

    private PutResult<K, V> putInternal(KeyStorage<K> keys, Children<K, V> children, K key, V val) {
        var lb = lowerBound(keys, key);
        var childIdx = lb.found() ? lb.idx() + 1 : lb.idx();
        var child = children.child(childIdx);

        return switch (put(child, key, val)) {
            case PutResult.NoSplit<K, V>(var node) -> new PutResult.NoSplit<>(
                    new Node.Internal<>(keys, children.insert(childIdx, node))
            );
            case PutResult.Split<K, V>(var left, var right, var promotedKey) -> {
                if (keys.size() < maxKeys) {
                    yield new PutResult.NoSplit<>(new Node.Internal<>(
                            keys.insert(childIdx, promotedKey),
                            children.insert(childIdx, left, right)
                    ));
                }
                var keySplit = keys.insertAndSplit(childIdx, splitAt, promotedKey);
                var childrenSplit = children.insertAndSplit(childIdx, splitAt, left, right);
                yield new PutResult.Split<>(
                        new Node.Internal<>(keySplit.left(), childrenSplit.left()),
                        new Node.Internal<>(keySplit.right(), childrenSplit.right()),
                        keySplit.promotedKey()
                );

            }
        };
    }



    private PutResult<K, V> putLeaf(KeyStorage<K> keys, ValueStorage<V> vals, K key, V val) {
        var lb = lowerBound(keys, key);
        if (lb.found()) {
            return new PutResult.NoSplit<>(new Node.Leaf<>(
                    keys,
                    vals.insert(lb.idx(), val)
            ));
        }

        if (keys.size() < maxKeys) {
            return new PutResult.NoSplit<>(new Node.Leaf<>(
                    keys.insert(lb.idx(), key),
                    vals.insert(lb.idx(), val)
            ));
        } else {
            var keySplit = keys.insertAndSplit(lb.idx(), splitAt, key);
            var valSplit = vals.insertAndSplit(lb.idx(), splitAt, val);

            var left = new Node.Leaf<>(keySplit.left(), valSplit.left());
            var right = new Node.Leaf<>(keySplit.right(), valSplit.right());

            return new PutResult.Split<>(
                    left,
                    right,
                    keySplit.promotedKey()
            );
        }
    }
}
