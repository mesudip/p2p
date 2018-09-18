package com.soriole.kademlia.core.store;

/**
 * TimestampedStore stores (key,value).
 * the value is wrapped around by the TimeStampedData class to put the timestamp information.
 *
 * @param <Type> The type of data to put
 */
public interface TimestampedStore<Type> {

    TimeStampedData<Type> put(Key k, Type value);

    TimeStampedData<Type> put(Key k, Type value, long expirationTime);

    TimeStampedData<Type> get(Key k);

    boolean refreshCreatedTime(Key k);

    TimeStampedData<Type> remove(Key k);

    TimeStampedData<Type> getFirstExpiring();

    TimeStampedData<Type> getFirstInserted();
}
