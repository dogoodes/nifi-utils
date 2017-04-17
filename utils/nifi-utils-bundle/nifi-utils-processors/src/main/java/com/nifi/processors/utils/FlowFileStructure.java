package com.nifi.processors.utils;

import com.google.gson.JsonElement;

/**
 * Created by Apple on 4/14/17.
 */
public class FlowFileStructure {

    private volatile JsonElement content;
    private volatile JsonElement attribute;

    public static final class Builder {

        private JsonElement content;
        private JsonElement attribute;

        public Builder content(JsonElement _content) {
            this.content = _content;
            return this;
        }

        public Builder attribute(JsonElement _attribute) {
            this.attribute = _attribute;
            return this;
        }

        public FlowFileStructure build() {
            return new FlowFileStructure(this);
        }
    }

    private FlowFileStructure(Builder builder) {
        this.content = builder.content;
        this.attribute = builder.attribute;
    }

    public JsonElement getContent() {
        return content;
    }

    public JsonElement getAttribute() {
        return attribute;
    }
}