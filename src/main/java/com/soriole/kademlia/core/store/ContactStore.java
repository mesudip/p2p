package com.soriole.kademlia.core.store;

import com.soriole.kademlia.core.KademliaConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class ContactStore implements Set<NodeInfo> {
    final BucketStore<Contact> contactBucketStore;
    final SortedSet<Contact> contactsByLastSeen = new TreeSet<>(Contact.getComparatorByLastActive());


    public ContactStore(NodeInfo localNode, KademliaConfig config) {
        contactBucketStore = new BucketStore<Contact>(localNode.getKey(), config.getKeyLength(), config.getK());
        contactBucketStore.put(localNode.getKey(), new Contact(localNode));
    }

    @Override
    public int size() {
        return contactBucketStore.size();
    }

    @Override
    public boolean isEmpty() {
        return contactBucketStore.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof NodeInfo) {
            return contactBucketStore.containsKey(((NodeInfo) o).getKey());
        }
        if (o instanceof Contact) {
            return contactBucketStore.containsKey(((Contact) o).getNodeInfo().getKey());
        } else if (o instanceof Key) {
            return contactBucketStore.containsKey(o);
        }
        throw new IllegalArgumentException();
    }

    @NotNull
    @Override
    public Iterator<NodeInfo> iterator() {
        contactBucketStore.entrySet().iterator().remove();
        final Iterator<Map.Entry<Key, Contact>> iterator = contactBucketStore.entrySet().iterator();
        return new Iterator<NodeInfo>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public NodeInfo next() {
                return iterator.next().getValue().getNodeInfo();
            }
        };
    }

    @NotNull
    @Override
    public Object[] toArray() {
        NodeInfo[] array = new NodeInfo[contactBucketStore.size()];
        int i = 0;
        for (Contact contact : contactBucketStore.values()) {
            array[i++] = contact.getNodeInfo();
        }
        return array;
    }

    @NotNull
    @Override
    public <T> T[] toArray(@NotNull T[] a) {
        T[] array = (T[]) new Object[contactBucketStore.size()];
        int i = 0;
        for (Contact contact : contactBucketStore.values()) {
            array[i++] = (T) contact.getNodeInfo();
        }
        return array;
    }

    @Override
    public boolean add(NodeInfo nodeInfo) {
        Contact c = new Contact(nodeInfo);
        if (contactBucketStore.put(nodeInfo.getKey(), c) == c) {
            return false;
        }
        return true;
    }

    @Override
    public boolean remove(Object o) {
        return contactBucketStore.remove(o) != null;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        for(Object object:c){
            if(object instanceof  Contact){
                if(!contactBucketStore.containsKey(((Contact) object).getNodeInfo().getKey())){
                    return false;
                }
            }
            if(object instanceof NodeInfo) {
                if (!contactBucketStore.containsKey(((NodeInfo) object).getKey())){
                    return false;
                }
            }
            if( object instanceof  Key){
                if(!contactBucketStore.containsKey(object)){
                    return false;
                }
            }

        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends NodeInfo> c) {
        boolean truth=true;
        for(NodeInfo nodeInfo:c){
            truth&=this.add(nodeInfo);
        }
        return truth;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        // TODO: do it effeciently.
        return false;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        for(Object object:c){
            if(object instanceof  Contact){
                contactBucketStore.remove(((Contact) object).getNodeInfo().getKey());
            }
            if(object instanceof NodeInfo) {
                contactBucketStore.remove(((NodeInfo) object).getKey());

            }
            if( object instanceof  Key){
                contactBucketStore.remove(object);
            }

        }
        return true;
    }

    @Override
    public void clear() {
        contactBucketStore.clear();
    }
}
