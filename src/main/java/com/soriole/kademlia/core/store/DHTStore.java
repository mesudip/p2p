package com.soriole.kademlia.core.store;

import com.soriole.kademlia.core.KademliaConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class DHTStore extends BucketStore<TimeStampedData<byte[]>> implements SortedMap<Key,TimeStampedData<byte[]>>,TimestampedStore<byte[]> {

    public DHTStore(Key localKey, KademliaConfig config) {
        super(localKey,config.getKeyLength(), config.getK());
    }

    @Override
    public TimeStampedData<byte[]> put(Key k, byte[] value) {
        return null;
    }

    @Override
    public TimeStampedData<byte[]> put(Key k, byte[] value, long expirationTime) {
        return null;
    }

    @Override
    public TimeStampedData<byte[]> get(Key k) {
        return null;
    }

    @Override
    public boolean refreshCreatedTime(Key k) {
        return false;
    }

    @Override
    public TimeStampedData<byte[]> remove(Key k) {
        return null;
    }

    @Override
    public TimeStampedData<byte[]> getFirstExpiring() {
        return null;
    }

    @Override
    public TimeStampedData<byte[]> getFirstInserted() {
        return null;
    }
}
