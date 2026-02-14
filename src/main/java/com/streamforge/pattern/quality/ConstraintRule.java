package com.streamforge.pattern.quality;

import java.io.Serializable;

@FunctionalInterface
public interface ConstraintRule<T> extends Serializable {

  String validate(T value);
}
