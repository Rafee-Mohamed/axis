package io.disys.axis.storage.swmr;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

// Single Writer Multi Reader ordered index
public class PersistentBTree<K, V> implements Iterable<KeyVal<K, V>>{
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
                var lb = Search.lowerBound(keys, key);
                var childIdx = lb.found() ? lb.idx() + 1 : lb.idx();
                yield get(children.child(childIdx), key);
            }

            case Node.Leaf<K, V>(var keys, var vals) -> {
                var lb = Search.lowerBound(keys, key);
                yield lb.found() ? vals.val(lb.idx()) : null;
            }
        };
    }

    // ====== RANGE ======

    List<KeyVal<K, V>> range(K from, K to) {
        var node = root;
        if (node == null) {
            return List.of();
        }

        var out = new ArrayList<KeyVal<K, V>>();
        range(node, from, to, out::add);
        return out;
    }

    <T extends Collection<KeyVal<K, V>>> T range(K from, K to, T out) {
        var node = root;
        if (node == null) {
            return out;
        }

        range(node, from, to, out::add);
        return out;
    }

    void range(K from, K to, Consumer<KeyVal<K, V>> consumer) {
        var node = root;
        if (node == null) {
            return;
        }

        range(node, from, to, consumer);
    }

    void range(Node<K, V> node, K from, K to, Consumer<KeyVal<K, V>>  consumer) {
        switch (node) {
            case Node.Internal<K, V>(var keys, var children) -> {
                var start = Search.lowerBound(keys, from).idx();
                var end = Search.lowerBound(keys, to).idx();

                // start > end returns
                for (var idx = start; idx <= end; idx++) {
                    range(children.child(idx), from, to, consumer);
                }
            }
            case Node.Leaf<K, V>(var keys, var vals) -> {
                var start = Search.lowerBound(keys, from).idx();
                var endLb = Search.lowerBound(keys, to);
                var end = endLb.found() ? endLb.idx() : endLb.idx() - 1;

                for (var idx = start; idx <= end; idx++) {
                    consumer.accept(KeyVal.of(keys.key(idx), vals.val(idx)));
                }
            }
        }
    }

    // ====== ITERATION ======

    @Override
    public Iterator<KeyVal<K, V>> iterator() {
        return BTreeIterator.of(root);
    }


    public Iterator<KeyVal<K, V>> rangeIterator(K from, K to) {
        return BoundedBTreeIterator.of(root, from, to);
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
        var lb = Search.lowerBound(keys, key);
        var childIdx = lb.found() ? lb.idx() + 1 : lb.idx();
        var child = children.child(childIdx);

        return switch (put(child, key, val)) {
            case PutResult.NoSplit<K, V>(var node) -> new PutResult.NoSplit<>(
                    new Node.Internal<>(keys, children.replace(childIdx, node))
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
        var lb = Search.lowerBound(keys, key);
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
