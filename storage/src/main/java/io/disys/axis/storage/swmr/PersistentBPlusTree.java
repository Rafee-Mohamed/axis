package io.disys.axis.storage.swmr;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

// Single Writer Multi Reader ordered index
public class PersistentBPlusTree<K, V> implements Iterable<KeyVal<K, V>>{
    private volatile Node<K, V> root;
    private final int maxKeys;
    // split at
    private final int minKeys;
    private final KeyStorageFactory<K> ksf;

    PersistentBPlusTree(int maxKeys, KeyStorageFactory<K> ksf) {
        this.maxKeys = maxKeys;
        this.minKeys = maxKeys / 2;
        this.ksf = ksf;
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

    V put(K key, V val) {
        var node = root;
        if (node == null) {
            root = new Node.Leaf<>(
                    ksf.single(key), ValueStorage.of(val)
            );
            return null;
        }

        var result = put(root, key, val);
        root = switch (result) {
            case PutResult.NoSplit<K, V>(var newRoot, _) -> newRoot;
            case PutResult.Split<K, V>(var left, var right, var promotedKey, _) ->
                    new Node.Internal<>(ksf.single(promotedKey), Children.of(left, right));
        };

        return result.replaced();
    }

    PutResult<K, V> put(Node<K, V> node, K key, V val) {
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
            case PutResult.NoSplit<K, V>(var node, var replaced) -> new PutResult.NoSplit<>(
                    new Node.Internal<>(keys, children.replace(childIdx, node)),
                    replaced
            );
            case PutResult.Split<K, V>(var left, var right, var promotedKey, var replaced) -> {
                if (keys.size() < maxKeys) {
                    yield new PutResult.NoSplit<>(new Node.Internal<>(
                            keys.insert(childIdx, promotedKey),
                            children.insert(childIdx, left, right)
                    ), replaced);
                }
                var keySplit = keys.insertAndSplit(childIdx, minKeys, promotedKey);
                var childrenSplit = children.insertAndSplit(childIdx, minKeys, left, right);
                yield new PutResult.Split<>(
                        new Node.Internal<>(keySplit.left(), childrenSplit.left()),
                        new Node.Internal<>(keySplit.right(), childrenSplit.right()),
                        keySplit.promotedKey(),
                        replaced
                );

            }
        };
    }

    private PutResult<K, V> putLeaf(KeyStorage<K> keys, ValueStorage<V> vals, K key, V val) {
        var lb = Search.lowerBound(keys, key);
        if (lb.found()) {
            var replaced = vals.val(lb.idx());
            return new PutResult.NoSplit<>(
                    new Node.Leaf<>(keys, vals.replace(lb.idx(), val)),
                    replaced
            );
        }

        if (keys.size() < maxKeys) {
            return new PutResult.NoSplit<>(new Node.Leaf<>(
                    keys.insert(lb.idx(), key),
                    vals.insert(lb.idx(), val)
            ), null);
        } else {
            var keySplit = keys.insertAndSplit(lb.idx(), minKeys, key);
            var valSplit = vals.insertAndSplit(lb.idx(), minKeys, val);

            var left = new Node.Leaf<>(keySplit.left(), valSplit.left());
            var right = new Node.Leaf<>(keySplit.right(), valSplit.right());

            return new PutResult.Split<>(
                    left,
                    right,
                    keySplit.promotedKey(),
                    null
            );
        }
    }

    // ====== REMOVE ======

    public V remove(K key) {
        var node = root;
        if (node == null) {
            return null;
        }
        var result = remove(node, key);
        root = switch (result) {
            case DeleteResult.NotFound<K, V> _ -> node;
            case DeleteResult.NoShrink<K, V>(var newRoot, _) -> newRoot;
            case DeleteResult.Shrink<K, V>(var newRoot, _) -> switch (newRoot) {
                case Node.Internal<K, V>(var keys, var children)
                        when keys.size() == 0 -> children.child(0);
                case Node.Leaf<K, V>(var keys, _)
                        when keys.size() == 0 -> null;
                default -> newRoot;
            };
        };
        return result.removed();
    }

    public DeleteResult<K, V> remove(Node<K, V> node, K key) {
        return switch (node) {
            case Node.Internal<K, V>(var keys, var children) -> removeInternal(keys, children, key);
            case Node.Leaf<K, V>(var keys, var vals) -> removeLeaf(keys, vals, key);
        };
    }

    private DeleteResult<K, V> removeInternal(KeyStorage<K> keys, Children<K, V> children, K key) {
        var lb = Search.lowerBound(keys, key);
        var idx = lb.found() ? lb.idx() + 1 : lb.idx();

        return switch (remove(children.child(idx), key)) {
            case DeleteResult.NotFound<K, V> nf -> nf;

            case DeleteResult.NoShrink<K, V>(var node, V removed) -> new DeleteResult.NoShrink<>(
                    new Node.Internal<>(keys, children.replace(idx, node)), removed
            );

            case DeleteResult.Shrink<K, V>(var node, V removed) -> {
                // try left borrow
                if (idx > 0 && canBorrow(children.child(idx - 1))) {
                    var donor = children.child(idx - 1);
                    var newNode =  switch (donor) {
                        case Node.Leaf<K, V> leafDonor ->
                                borrowFromLeftSibling((Node.Leaf<K, V>) node, leafDonor, keys, children, idx - 1);
                        case Node.Internal<K, V> internalDonor ->
                                borrowFromLeftSibling((Node.Internal<K, V>) node, internalDonor, keys, children, idx - 1);
                    };

                    yield new DeleteResult.NoShrink<>(newNode, removed);
                }

                // try right borrow
                if (idx < children.size() - 1 && canBorrow(children.child(idx + 1))) {
                    var donor = children.child(idx + 1);
                    var newNode = switch (donor) {
                        case Node.Leaf<K, V> leafDonor ->
                                borrowFromRightSibling((Node.Leaf<K, V>) node, leafDonor, keys, children, idx);
                        case Node.Internal<K, V> internalDonor ->
                                borrowFromRightSibling((Node.Internal<K, V>) node, internalDonor, keys, children, idx);
                    };

                    yield new DeleteResult.NoShrink<>(newNode, removed);
                }

                // else merge
                var newNode = switch (node) {
                    case Node.Leaf<K, V> rightLeaf when idx > 0 -> merge(
                            (Node.Leaf<K, V>) children.child(idx - 1),
                            rightLeaf,
                            keys,
                            children,
                            idx - 1
                    );
                    case Node.Leaf<K, V> leftLeaf -> merge(
                            leftLeaf,
                            (Node.Leaf<K, V>) children.child(idx + 1),
                            keys,
                            children,
                            idx
                    );
                    case Node.Internal<K, V> rightInternal when idx > 0 -> merge(
                            (Node.Internal<K, V>) children.child(idx - 1),
                            rightInternal,
                            keys,
                            children,
                            idx - 1
                    );
                    case Node.Internal<K, V> leftInternal -> merge(
                            leftInternal,
                            (Node.Internal<K, V>) children.child(idx + 1),
                            keys,
                            children,
                            idx
                    );
                };

                yield underflows(newNode.keys())
                        ? new DeleteResult.Shrink<>(newNode, removed)
                        : new DeleteResult.NoShrink<>(newNode, removed);

            }
        };
    }

    private Node.Internal<K, V> merge(
            Node.Internal<K, V> left,
            Node.Internal<K, V> right,
            KeyStorage<K> keys,
            Children<K, V> children,
            int parentIdx
    ) {
        var leftKeys = left.keys();
        var leftChildren = left.children();

        var rightKeys = right.keys();
        var rightChildren = right.children();

        var separatorKey = keys.key(parentIdx);

        var mergedKeys = leftKeys.insertAndMerge(leftKeys.size(), separatorKey, rightKeys);
        var mergedChildren = leftChildren.merge(rightChildren);
        var mergedNode = new Node.Internal<>(mergedKeys, mergedChildren);

        var newKeys = keys.remove(parentIdx);
        var newChildren = children.removeAndReplace(parentIdx, mergedNode);

        return new Node.Internal<>(newKeys, newChildren);
    }

    private Node.Internal<K, V> merge(
            Node.Leaf<K, V> left,
            Node.Leaf<K, V> right,
            KeyStorage<K> keys,
            Children<K, V> children,
            int parentIdx
    ) {
        var leftKeys = left.keys();
        var leftVals = left.values();

        var rightKeys = right.keys();
        var rightVals = right.values();

        var mergedKeys = leftKeys.merge(rightKeys);
        var mergedValues = leftVals.merge(rightVals);
        var mergedNode = new Node.Leaf<>(mergedKeys, mergedValues);

        var newKeys = keys.remove(parentIdx);
        var newChildren = children.removeAndReplace(parentIdx, mergedNode);

        return new Node.Internal<>(newKeys, newChildren);
    }

    private Node.Internal<K, V> borrowFromLeftSibling(
            Node.Internal<K, V> borrower,
            Node.Internal<K, V> donor,
            KeyStorage<K> keys,
            Children<K, V> children,
            int parentIdx
    ) {

        var donorKeys = donor.keys();
        var donorChildren = donor.children();

        var borrowerKeys = borrower.keys();
        var borrowerChildren = borrower.children();

        var depromotedKey = keys.key(parentIdx);

        var last = donorKeys.size() - 1;
        var promotedKey = donorKeys.key(last);
        var borrowedChild = donorChildren.child(last + 1);

        var leftKeys = donorKeys.remove(last);
        var leftChildren = donorChildren.remove(last + 1);
        var leftNode = new Node.Internal<>(leftKeys, leftChildren);

        var rightKeys = borrowerKeys.insert(0, depromotedKey);
        var rightChildren = borrowerChildren.insert(0, borrowedChild);
        var rightNode = new Node.Internal<>(rightKeys, rightChildren);

        var newKeys = keys.replace(parentIdx, promotedKey);
        var newChildren = children.replace(parentIdx, leftNode, rightNode);

        return new Node.Internal<>(newKeys, newChildren);
    }

    private Node.Internal<K, V> borrowFromRightSibling(
            Node.Internal<K, V> borrower,
            Node.Internal<K, V> donor,
            KeyStorage<K> keys,
            Children<K, V> children,
            int parentIdx
    ) {

        var donorKeys = donor.keys();
        var donorChildren = donor.children();

        var borrowerKeys = borrower.keys();
        var borrowerChildren = borrower.children();

        var depromotedKey = keys.key(parentIdx);

        var promotedKey = donorKeys.key(0);
        var borrowedChild = donorChildren.child(0);

        var rightKeys = donorKeys.remove(0);
        var rightChildren = donorChildren.remove(0);
        var rightNode = new Node.Internal<>(rightKeys, rightChildren);

        var leftKeys = borrowerKeys.insert(borrowerKeys.size(), depromotedKey);
        var leftChildren = borrowerChildren.insert(borrowerChildren.size(), borrowedChild);
        var leftNode = new Node.Internal<>(leftKeys, leftChildren);

        var newKeys = keys.replace(parentIdx, promotedKey);
        var newChildren = children.replace(parentIdx, leftNode, rightNode);

        return new Node.Internal<>(newKeys, newChildren);
    }

    private Node.Internal<K, V> borrowFromLeftSibling(
            Node.Leaf<K, V> borrower,
            Node.Leaf<K, V> donor,
            KeyStorage<K> keys,
            Children<K, V> children,
            int parentIdx
    ) {

        var donorKeys = donor.keys();
        var donorVals = donor.values();

        var last = donorKeys.size() - 1;
        var borrowedKey = donorKeys.key(last);
        var borrowedVal = donorVals.val(last);

        var leftKeys = donorKeys.remove(last);
        var leftVals = donorVals.remove(last);
        var leftNode = new Node.Leaf<>(leftKeys, leftVals);

        var rightKeys = borrower.keys().insert(0, borrowedKey);
        var rightVals = borrower.values().insert(0, borrowedVal);
        var rightNode = new Node.Leaf<>(rightKeys, rightVals);

        var newKeys = keys.replace(parentIdx, rightKeys.key(0));
        var newChildren = children.replace(parentIdx, leftNode, rightNode);

        return new Node.Internal<>(newKeys, newChildren);
    }

    private Node.Internal<K, V> borrowFromRightSibling(
            Node.Leaf<K, V> borrower,
            Node.Leaf<K, V> donor,
            KeyStorage<K> keys,
            Children<K, V> children,
            int parentIdx
    ) {

        var donorKeys = donor.keys();
        var donorVals = donor.values();

        var borrowedKey = donorKeys.key(0);
        var borrowedVal = donorVals.val(0);

        var rightKeys = donorKeys.remove(0);
        var rightVals = donorVals.remove(0);
        var rightNode = new Node.Leaf<>(rightKeys, rightVals);

        var leftKeys = borrower.keys()
                .insert(borrower.keys().size(), borrowedKey);
        var leftVals = borrower.values()
                .insert( borrower.values().size(), borrowedVal);
        var leftNode = new Node.Leaf<>(leftKeys, leftVals);

        var newKeys = keys.replace(parentIdx, rightKeys.key(0));
        var newChildren = children.replace(parentIdx, leftNode, rightNode);

        return new Node.Internal<>(newKeys, newChildren);
    }

    private boolean canBorrow(Node<K, V> node) {
        return node.keys().size() > minKeys;
    }

    private boolean underflows(KeyStorage<K> keys) {
        return keys.size() < minKeys;
    }


    private DeleteResult<K, V> removeLeaf(KeyStorage<K> keys, ValueStorage<V> vals, K key) {
        var lb = Search.lowerBound(keys, key);

        if (!lb.found()) {
            return new DeleteResult.NotFound<>();
        }

        var idx = lb.idx();
        var removed = vals.val(idx);
        var newKeys = keys.remove(idx);
        var newVals = vals.remove(idx);
        var newLeaf = new Node.Leaf<>(newKeys, newVals);

        return underflows(newKeys)
                ? new DeleteResult.Shrink<>(newLeaf, removed)
                : new DeleteResult.NoShrink<>(newLeaf, removed);
    }
}
