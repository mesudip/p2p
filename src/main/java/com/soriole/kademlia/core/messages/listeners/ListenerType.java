package com.soriole.kademlia.core.messages.listeners;

import com.soriole.kademlia.core.messages.Message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ListenerType {
    Class<?extends Message> messageClass();
}
