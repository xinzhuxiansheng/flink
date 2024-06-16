package com.yzhou.blog.wordcount.checkpoint;

import java.util.List;


public class KeyedState {
    public String key;

    public Long value;

    @Override
    public String toString() {
        return "KeyedState{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}

