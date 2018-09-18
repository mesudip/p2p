package com.soriole.kademlia.core.store;

import com.soriole.kademlia.core.util.BoundedSortedSet;
import com.soriole.kademlia.core.util.KeyComparatorByBucketPosition;
import com.soriole.kademlia.core.util.KeyComparatorByDistance;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;


/**
 * BucketStore is a SortedMap, that stores data in multiple SortedMaps internally.
 * The no of SortedMaps to use is given by the bucketCount+1.
 * Each SortedMap further has a limited sized given by the bucketSize.
 * The Sorting of entries in the map is obtained by the xor metric calculation.
 * <p>
 * Example of a store.
 * [0]      --> [a]
 * [1]      --> [b]
 * [2]      --> [c,f]
 * [3]      --> [g,c,d,e]
 * [4]      --> []
 * .
 * .
 * .
 * [bucket size]--> [x,z]
 *
 * @param <Type>
 */
public class BucketStore<Type> implements SortedMap<Key, Type> {

    final private SortedMap<Key, Type>[] buckets;
    final private Comparator<? super Key> comparator;

    final private Key localKey;
    final private int bucketSize;
    private int size = 0;

    public BucketStore(Key localKey, Comparator<? super Key> comparator, int bucketCount, int bucketSize) {
        this.localKey = localKey;
        this.bucketSize = bucketSize;
        // the buckets are kept by index starting from  1.
        // the zeroth index is for keeping localNode's mapping.
        buckets = new SortedMap[bucketCount + 1];
        this.comparator = comparator;
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new TreeMap<Key, Type>(comparator);
        }
    }

    public BucketStore(Key localKey, int bucketCount, int bucketSize) {
        this(localKey, new KeyComparatorByBucketPosition(localKey), bucketCount, bucketSize);
    }

    @Override
    public Comparator<? super Key> comparator() {
        return comparator;
    }

    @NotNull
    @Override
    public SortedMap<Key, Type> subMap(Key fromKey, Key toKey) {
        return new SubBucketStore(fromKey, toKey);
    }

    @NotNull
    @Override
    public SortedMap<Key, Type> headMap(Key toKey) {

        return new SubBucketStore(localKey, toKey);
    }

    @NotNull
    @Override
    public SortedMap<Key, Type> tailMap(Key fromKey) {

        return new SubBucketStore(fromKey, localKey.farthestKey());
    }

    @Override
    public synchronized Key firstKey() throws NoSuchElementException {
        for (SortedMap<Key, Type> bucket : buckets) {
            if (bucket.size() > 0) {
                return bucket.firstKey();
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public synchronized Key lastKey() {
        for (int i = buckets.length - 1; i > -1; i++) {
            if (buckets[i].size() > 0) {
                return buckets[i].lastKey();
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            throw new NullPointerException();
        }
        return buckets[localKey.getBucketPosition((Key) key)].containsKey(key);
    }

    @Override
    public synchronized boolean containsValue(Object value) {
        if (value == null) {
            throw new NullPointerException();
        } else {
            for (SortedMap<Key, Type> bucket : buckets) {
                if (bucket.containsValue(value)) {
                    return true;
                }
            }
        }
        return false;
    }


    @Override
    public synchronized Type get(Object key) {
        return (this.buckets[localKey.getBucketPosition((Key) key)]).get(key);
    }

    @Nullable
    @Override
    public synchronized Type put(Key key, Type value) throws IndexOutOfBoundsException {
        int distance = localKey.getBucketPosition(key);
        SortedMap<Key, Type> bucket = buckets[distance];

        Type put = bucket.put(key, value);

        if (bucket.size() > bucketSize) {
            bucket.remove(key);
            return value;
        } else {
            if (put == null) {
                size++;
            }
            return put;
        }
    }

    @Override
    public Type remove(Object key) {
        int distance = localKey.getBucketPosition((Key) key);
        Type val = buckets[distance].remove(key);
        if (val != null) {
            size--;
        }
        return val;
    }

    @Override
    public void putAll(@NotNull Map<? extends Key, ? extends Type> m) {
        for (Key k : m.keySet()) {
            this.put(k, m.get(k));
        }
    }

    @Override
    public synchronized void clear() {
        for (int i = 1; i < buckets.length; i++) {
            buckets[i].clear();
        }
        size = 0;
    }

    @NotNull
    @Override
    public Set<Key> keySet() {
        return new BucketKeySet(this);
    }

    @NotNull
    @Override
    public Collection<Type> values() {
        return new BucketValueCollection(this);
    }

    @NotNull
    @Override
    public Set<Entry<Key, Type>> entrySet() {
        return new bucketEntrySet(this);
    }

    public Set<Entry<Key, Type>> entrySetForBucketOf(Key key) {
        return buckets[localKey.getBucketPosition(key)].entrySet();
    }

    public Set<Entry<Key, Type>> getClosestEntriesTo(final Key key, int count) {
        Entry.comparingByKey();
        BoundedSortedSet<Entry<Key, Type>> closestEntries = new BoundedSortedSet<Entry<Key, Type>>(count, new Comparator<Entry<Key, Type>>() {
            KeyComparatorByDistance comparatorByDistance = new KeyComparatorByDistance(key);

            @Override
            public int compare(Entry<Key, Type> o1, Entry<Key, Type> o2) {
                return comparatorByDistance.compare(o1.getKey(), o2.getKey());
            }
        });

        int pos = localKey.getBucketPosition(key);
        if (pos == 0) {
            pos++;
        }
        // put all nodes in that bucket to the list
        closestEntries.addAll(buckets[pos].entrySet());
        int uperLimit = buckets.length - 1;

        for (int i = 0; !closestEntries.isFull(); i++) {
            // if we have reached the begining of bucketList already
            if (pos == 0) {
                while (!closestEntries.isFull() && i < buckets.length) {
                    closestEntries.addAll(buckets[i].entrySet());
                    i++;
                }
                break;

            }
            // if ew have reached the end of bucketList already
            else if (pos == uperLimit) {
                i = pos - i;
                while (!closestEntries.isFull() && i > -1) {
                    closestEntries.addAll(buckets[i].entrySet());
                    i--;
                }
                break;

            }

            closestEntries.addAll(buckets[pos - i].entrySet());
            closestEntries.addAll(buckets[pos + i].entrySet());


        }
        return closestEntries;
    }

    class bucketEntrySet implements Set<Entry<Key, Type>> {
        final SortedMap<Key, Type>[] buckets = BucketStore.this.buckets;
        SortedMap<Key, Type> backedCollection;

        bucketEntrySet(SortedMap<Key, Type> store) {
            backedCollection = store;
        }

        @Override
        public int size() {
            return backedCollection.size();
        }

        @Override
        public boolean isEmpty() {
            return backedCollection.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if (o instanceof Entry)
                return backedCollection.get(((Entry<Key, Type>) o).getKey()).equals(((Entry<Key, Type>) o).getValue());
            else if (o instanceof Key) {
                return backedCollection.containsKey(o);
            }
            throw new IllegalArgumentException();
        }

        @NotNull
        @Override
        public Iterator<Entry<Key, Type>> iterator() {
            return new Iterator<Entry<Key, Type>>() {
                int pos = 0;
                Iterator<Entry<Key, Type>> currentIterator = buckets[0].entrySet().iterator();

                @Override

                public boolean hasNext() {
                    synchronized (BucketStore.this) {
                        while (!currentIterator.hasNext()) {
                            pos++;
                            if (pos == buckets.length) {
                                return false;
                            }
                            currentIterator = buckets[pos++].entrySet().iterator();

                        }
                        return true;
                    }
                }

                @Override
                public Entry<Key, Type> next() {
                    synchronized (BucketStore.this) {
                        if (this.hasNext()) {
                            return currentIterator.next();
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                }

                @Override
                public void remove() {
                    synchronized (BucketStore.this) {
                        size--;
                        currentIterator.remove();
                    }
                }

            };
        }

        @NotNull
        @Override
        public Object[] toArray() {
            synchronized (BucketStore.this) {
                Object[] array = new Object[size()];
                int i = 0;
                for (Entry<Key, Type> entry : this) {
                    array[i] = entry;
                }
                return array;
            }
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {
            synchronized (BucketStore.this) {
                if (a instanceof Entry[]) {
                    Object[] array = new Entry[size()];
                    int i = 0;
                    for (Entry<Key, Type> entry : this) {
                        array[i] = entry;
                    }
                    return (T[]) array;
                } else {
                    throw new ArrayStoreException();
                }
            }
        }

        @Override
        public boolean add(Entry<Key, Type> keyTypeEntry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Entry) {
                return backedCollection.remove(((Entry) o).getKey()) != null;
            } else if (o instanceof Key) {
                return backedCollection.remove(o) != null;
            }
            throw new IllegalArgumentException();
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            synchronized (BucketStore.this) {
                for (Object entry : c) {
                    if (entry instanceof Entry) {
                        if (!BucketStore.this.get(((Entry) entry).getKey()).equals(((Entry) entry).getValue())) {
                            return false;
                        }
                    } else if (entry instanceof Key) {
                        if (!BucketStore.this.containsKey(entry)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends Entry<Key, Type>> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            boolean modified = false;
            if (!(c instanceof Set)) {
                c = new HashSet<>(c);
            }
            synchronized (BucketStore.this) {

                Iterator<Entry<Key, Type>> iterator = this.iterator();
                while (iterator.hasNext()) {
                    Entry<Key, Type> entry = iterator.next();
                    if (!(c.contains(entry) || c.contains(entry.getKey()))) {
                        iterator.remove();
                        modified = true;
                    }
                }
            }
            return modified;
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            boolean modified = false;

            int max = this.size();
            if (this.size() < BucketStore.this.buckets.length) {
                max = BucketStore.this.buckets.length;
            }
            if (c.size() > max) {
                if (!(c instanceof Set)) {
                    c = new HashSet<>(c);
                }
                synchronized (BucketStore.this) {
                    Iterator<Entry<Key, Type>> iterator = this.iterator();
                    while (iterator.hasNext()) {
                        Entry<Key, Type> entry = iterator.next();
                        if (c.contains(entry) || c.contains(entry.getKey())) {
                            iterator.remove();
                            modified = true;
                        }
                    }
                }
            } else {
                synchronized (this) {
                    for (Object o : c) {
                        this.remove(o);
                    }
                }
            }
            return modified;
        }

        @Override
        public void clear() {
            backedCollection.clear();
        }
    }

    class BucketKeySet implements Set<Key> {

        SortedMap<Key, Type> parent;

        BucketKeySet(SortedMap<Key, Type> parent) {
            this.parent = parent;
        }

        @Override
        public int size() {
            return parent.size();
        }

        @Override
        public boolean isEmpty() {
            return parent.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return parent.containsKey(o);
        }

        @NotNull
        @Override
        public Iterator<Key> iterator() {
            return new Iterator<Key>() {
                Iterator<Entry<Key, Type>> parentIterator = parent.entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return parentIterator.hasNext();
                }

                @Override
                public Key next() {
                    return parentIterator.next().getKey();
                }

                @Override
                public void remove() {
                    parentIterator.remove();
                }
            };
        }


        @NotNull
        @Override
        public Object[] toArray() {

            Object[] array = new Object[size()];
            int i = 0;
            synchronized (BucketStore.this) {
                for (Key o : this) {
                    array[i++] = o;
                }
                return array;
            }
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {

            if (a.length < size()) {

                a = (T[]) new Object[size()];
            }
            int i = 0;
            synchronized (BucketStore.this) {
                for (Object o : this) {
                    a[i++] = (T) o;
                }
                return a;
            }
        }

        @Override
        public boolean add(Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            return parent.remove(o) != null;
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            synchronized (BucketStore.this) {
                for (Object o : c) {
                    if (!parent.containsKey(o)) {
                        return false;
                    }
                }
                return true;
            }
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends Key> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            synchronized (BucketStore.this) {
                boolean collectionChanged = false;
                Iterator<Entry<Key, Type>> iterator = parent.entrySet().iterator();
                if (!(c instanceof Set)) {
                    c = new HashSet<>(c);
                }
                while (iterator.hasNext()) {
                    if (c.contains(iterator.next().getKey())) {
                        iterator.remove();
                        collectionChanged = true;
                    }
                }
                return collectionChanged;
            }
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            synchronized (BucketStore.this) {
                boolean changed = false;
                for (Object o : c) {
                    changed |= parent.remove(o) != null;
                }
                return changed;
            }
        }

        @Override
        public void clear() {
            parent.clear();
        }
    }

    class BucketValueCollection implements Collection<Type> {
        SortedMap<Key, Type> parent;

        BucketValueCollection(SortedMap<Key, Type> parent) {
            this.parent = parent;
        }

        @Override
        public int size() {
            return parent.size();
        }

        @Override
        public boolean isEmpty() {
            return parent.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return parent.containsValue(o);
        }

        @NotNull
        @Override
        public Iterator<Type> iterator() {
            return new Iterator<Type>() {
                Iterator<Entry<Key, Type>> parentIterator = parent.entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return parentIterator.hasNext();
                }

                @Override
                public Type next() {
                    return parentIterator.next().getValue();
                }

                @Override
                public void remove() {
                    parentIterator.remove();
                }
            };
        }

        @NotNull
        @Override
        public Object[] toArray() {
            synchronized (BucketStore.this) {
                Object[] array = new Object[size()];
                int i = 0;
                for (Type o : this) {
                    array[i++] = o;
                }
                return array;
            }
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {
            synchronized ((BucketStore.this)) {
                if (a.length < size()) {

                    a = (T[]) new Object[size()];
                }
                int i = 0;
                for (Object o : this) {
                    a[i++] = (T) o;
                }
                return a;
            }
        }

        @Override
        public boolean add(Type type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            synchronized (BucketStore.this) {
                boolean removed = false;
                Iterator<Type> iterator = iterator();
                while (iterator.hasNext()) {
                    if (iterator.next().equals(o)) {
                        iterator.remove();
                        removed = true;
                    }
                }
                return removed;
            }
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            synchronized (BucketStore.this) {
                c = new HashSet<>(c);
                c.removeAll(this);
                return c.size() == 0;
            }


        }

        @Override
        public boolean addAll(@NotNull Collection<? extends Type> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            if (!(c instanceof Set)) {
                c = new HashSet<>(c);
            }
            boolean collectionChanged = false;
            synchronized (BucketStore.this) {
                Iterator<Type> iterator = iterator();
                while (iterator.hasNext()) {
                    if (c.contains(iterator.next())) {
                        iterator.remove();
                        collectionChanged = true;
                    }
                }
            }
            return collectionChanged;
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            if (!(c instanceof Set)) {
                c = new HashSet<>(c);
            }
            boolean collectionChanged = false;
            synchronized (BucketStore.this) {
                Iterator<Type> iterator = iterator();
                while (iterator.hasNext()) {
                    if (!c.contains(iterator.next())) {
                        iterator.remove();
                        collectionChanged = true;
                    }
                }
            }
            return collectionChanged;
        }

        @Override
        public void clear() {
            parent.clear();
        }
    }

    public class SubBucketStore implements SortedMap<Key, Type> {
        final BucketStore<Type> store = BucketStore.this;
        Key fromKey;
        Key toKey;
        int startIndex = store.localKey.getBucketPosition(fromKey);
        int endIndex = store.localKey.getBucketPosition(toKey);

        public SubBucketStore(Key fromKey, Key toKey) {
            this.fromKey = fromKey;
            this.toKey = toKey;
            int startIndex = localKey.getBucketPosition(fromKey);
            int endIndex = localKey.getBucketPosition(toKey);

            if (endIndex >= buckets.length) {
                throw new IllegalArgumentException();
            }
            if (startIndex < 0) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public int size() {
            synchronized (store) {
                int size = store.buckets[startIndex].tailMap(fromKey).size();
                for (int i = startIndex + 1; i <= endIndex; i++) {
                    size += store.buckets[i].size();
                }
                size += store.buckets[endIndex].headMap(toKey).size();
                return size;
            }
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            synchronized (store) {
                if (store.buckets[startIndex].tailMap(fromKey).containsKey(key)) {
                    return true;
                }
                for (int i = startIndex + 1; i <= endIndex; i++) {
                    if (store.buckets[i].containsKey(key)) {
                        return true;
                    }
                }
                if (store.buckets[endIndex].headMap(toKey).containsKey(key)) {
                    return true;
                }
            }
            return false;

        }

        @Override
        public boolean containsValue(Object value) {
            synchronized (store) {
                if (store.buckets[startIndex].tailMap(fromKey).containsValue(value)) {
                    return true;
                }
                for (int i = startIndex + 1; i <= endIndex; i++) {
                    if (store.buckets[i].containsValue(value)) {
                        return true;
                    }
                }
                if (store.buckets[endIndex].headMap(toKey).containsValue(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Type get(Object _key) {
            if (_key == null) {
                throw new NullPointerException();
            }

            Key key = (Key) _key;
            int position = store.localKey.getBucketPosition(key);
            synchronized (store) {
                if (position <= startIndex) {
                    return store.buckets[startIndex].tailMap(fromKey).get(key);
                } else if (position < endIndex) {
                    return store.buckets[position].get(key);
                } else {
                    return store.buckets[endIndex].headMap(toKey).get(key);
                }
            }
        }

        @Nullable
        @Override
        public Type put(Key key, Type value) {
            int position = store.localKey.getBucketPosition(key);
            Type oldValue;
            synchronized (store) {

                if (position < startIndex || position > endIndex) {
                    throw new IllegalArgumentException();
                } else if (position == startIndex) {
                    oldValue = store.buckets[startIndex].tailMap(fromKey).put(key, value);
                } else if (position == endIndex) {
                    oldValue = store.buckets[startIndex].put(key, value);

                } else {
                    oldValue = store.buckets[endIndex].headMap(toKey).put(key, value);
                }
                if (store.buckets[position].size() > bucketSize) {
                    store.buckets[position].remove(key);
                    return value;
                }
                if (oldValue == null) {
                    BucketStore.this.size++;
                }

                return oldValue;
            }
        }

        @Override
        public Type remove(Object key) {
            int position = store.localKey.getBucketPosition((Key) key);
            Type oldValue;
            synchronized (store) {

                if (position < startIndex || position > endIndex) {
                    throw new IllegalArgumentException();
                } else if (position == startIndex) {
                    oldValue = store.buckets[startIndex].tailMap(fromKey).remove(key);
                } else if (position == endIndex) {
                    oldValue = store.buckets[startIndex].remove(key);

                } else {
                    oldValue = store.buckets[endIndex].headMap(toKey).remove(key);
                }
                if(oldValue!=null){
                    size--;
                    return oldValue;
                }
                return null;

            }
        }

        @Override
        public void putAll(@NotNull Map<? extends Key, ? extends Type> m) {
            synchronized (store) {
                for (Entry<? extends Key, ? extends Type> entry : m.entrySet()) {
                    try {
                        put(entry.getKey(), entry.getValue());
                    } catch (IllegalArgumentException | NullPointerException ignored) {
                    }
                }
            }
        }

        @Override
        public void clear() {
            synchronized (store) {
                size=size-buckets[startIndex].size()-buckets[startIndex].size();
                store.buckets[startIndex].tailMap(fromKey).clear();
                store.buckets[endIndex].headMap(toKey).clear();
                size+=buckets[startIndex].size()-buckets[endIndex].size();
                for (int i = startIndex + 1; i < endIndex; i++) {
                    size-=buckets[i].size();
                    store.buckets[i].clear();
                }
            }
        }

        @Override
        public Comparator<? super Key> comparator() {
            return store.comparator();
        }

        @NotNull
        @Override
        public SortedMap<Key, Type> subMap(Key fromKey, Key toKey) {
            if (localKey.calculateDistance(fromKey).compareTo(localKey.calculateDistance(this.fromKey)) < 0) {
                throw new IllegalArgumentException();
            }
            if (localKey.calculateDistance(toKey).compareTo(localKey.calculateDistance(this.toKey)) > 0) {
                throw new IllegalArgumentException();
            }
            return store.subMap(fromKey, toKey);
        }

        @NotNull
        @Override
        public SortedMap<Key, Type> headMap(Key toKey) {
            if (localKey.calculateDistance(toKey).compareTo(localKey.calculateDistance(this.toKey)) > 0) {
                throw new IllegalArgumentException();
            }
            return store.subMap(fromKey, toKey);
        }

        @NotNull
        @Override
        public SortedMap<Key, Type> tailMap(Key fromKey) {
            if (localKey.calculateDistance(fromKey).compareTo(localKey.calculateDistance(this.fromKey)) > 0) {
                throw new IllegalArgumentException();
            }
            return store.subMap(fromKey, toKey);
        }

        @Override
        public Key firstKey() {
            synchronized (store) {
                SortedMap<Key, Type> tailMap = store.buckets[startIndex].tailMap(fromKey);
                if (tailMap.size() > 0) {
                    return tailMap.firstKey();
                } else {
                    int i = startIndex;
                    while (buckets[i].size() == 0 && i < endIndex) {
                        i++;
                    }
                    if (i < endIndex) {
                        return store.buckets[i].firstKey();
                    } else {
                        return store.buckets[endIndex].tailMap(toKey).firstKey();
                    }
                }
            }
        }

        @Override
        public Key lastKey() {
            synchronized (store) {
                SortedMap<Key, Type> headMap = store.buckets[startIndex].headMap(toKey);
                if (headMap.size() > 0) {
                    return headMap.firstKey();
                } else {
                    int i = endIndex;
                    while (buckets[i].size() == 0 && i > startIndex) {
                        i--;
                    }
                    if (i > startIndex) {
                        return store.buckets[i].firstKey();
                    } else {
                        return store.buckets[endIndex].headMap(fromKey).firstKey();
                    }
                }
            }
        }

        @NotNull
        @Override
        public Set<Key> keySet() {
            return new BucketKeySet(this);
        }

        @NotNull
        @Override
        public Collection<Type> values() {
            return new BucketValueCollection(this);
        }

        @NotNull
        @Override
        public Set<Entry<Key, Type>> entrySet() {
            return new partialBucketEntrySet(this);
        }

        class partialBucketEntrySet extends bucketEntrySet {

            partialBucketEntrySet(SortedMap<Key, Type> store) {
                super(store);
            }

            @NotNull
            @Override
            public Iterator<Entry<Key, Type>> iterator() {
                return new Iterator<Entry<Key, Type>>() {
                    Iterator<Entry<Key, Type>> currentIterator = buckets[startIndex].tailMap(fromKey).entrySet().iterator();

                    int position = startIndex;

                    @Override
                    public boolean hasNext() {
                        synchronized (store) {
                            while (!currentIterator.hasNext()) {
                                position++;
                                if (position >= endIndex) {
                                    if (position == endIndex) {
                                        currentIterator = buckets[endIndex].headMap(toKey).entrySet().iterator();
                                    } else {
                                        return false;
                                    }
                                }
                                currentIterator = buckets[position++].entrySet().iterator();

                            }
                        }
                        return true;

                    }

                    @Override
                    public Entry<Key, Type> next() {
                        synchronized (store) {
                            if (hasNext()) {
                                return currentIterator.next();
                            }
                        }
                        throw new NoSuchElementException();
                    }

                    @Override
                    public void remove() {
                        synchronized (store) {
                            currentIterator.remove();
                        }
                    }
                };
            }
        }
    }

}
