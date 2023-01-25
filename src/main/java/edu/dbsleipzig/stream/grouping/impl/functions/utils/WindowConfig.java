/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.dbsleipzig.stream.grouping.impl.functions.utils;

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.internal.BaseExpressions;

import static org.apache.flink.table.api.Expressions.lit;

/**
 * Helper class for defining the window configuration.
 */
public class WindowConfig {
  /**
   * The default numerical value for the window size.
   */
  private static final int DEFAULT_VALUE = 10;
  /**
   * The default time unit.
   */
  private static final TimeUnit DEFAULT_UNIT = TimeUnit.SECONDS;
  /**
   * Holds the value of the window size.
   */
  private int value = DEFAULT_VALUE;
  /**
   * Holds the unit of the window size, e.g., seconds, hours, ...
   */
  private TimeUnit unit = DEFAULT_UNIT;

  /**
   * Creates an instance of the window config. Since this is private, please use the static functions
   * {@link WindowConfig#create()} to initialize a config with default window specification (10 seconds).
   */
  private WindowConfig() {
  }

  /**
   * Creates an instance of the window config with default window specification (10 seconds).
   *
   * @return an instance of this window configuration with a default window size of 10 seconds
   */
  public static WindowConfig create() {
    return new WindowConfig();
  }

  /**
   * Returns an API expression for the Table-API window definition to use as time definition here:
   * {@code table.window(Tumble.over( -- here --)}
   *
   * @return an ApiExpression object with the numerical value as literal using {@link Expressions#lit(Object)}
   *         and the corresponding time unit like {@link BaseExpressions#seconds()}.
   */
  public ApiExpression getWindowExpression() {
    switch (unit) {
    case SECONDS:
      return lit(value).seconds();
    case MINUTES:
      return lit(value).minutes();
    case HOURS:
      return lit(value).hours();
    case DAYS:
      return lit(value).days();
    default:
      throw new IllegalArgumentException("Illegal time unit for window.");
    }
  }

  /**
   * Returns the window definition as String for usage in SQL Api.
   *
   * @return the window definition as String for usage in SQL Api, e.g. " INTERVAL '10' SECONDS" or
   * " INTERVAL '1' DAYS"
   */
  public String getSqlApiExpression() {
    String expression = " INTERVAL '" + this.value + "' ";
    switch (unit) {
    case SECONDS:
      expression += "SECONDS";
      break;
    case MINUTES:
      expression += "MINUTES";
      break;
    case HOURS:
      expression += "HOURS";
      break;
    case DAYS:
      expression += "DAYS";
      break;
    default:
      throw new IllegalArgumentException("Illegal time unit for window.");
    }
    return expression;
  }

  /**
   * Set the window size time value. Use {@link WindowConfig#setUnit(TimeUnit)} to set the corresponding
   * time unit.
   *
   * @param value the window size value, e.g., 10
   * @return this config object
   */
  public WindowConfig setValue(int value) {
    this.value = value;
    return this;
  }

  /**
   * Set the window size time unit. Use {@link WindowConfig#setValue(int)} to set the corresponding
   * time value.
   *
   * @param unit the window size unit, e.g., {@link TimeUnit#MINUTES}.
   * @return this config object
   */
  public WindowConfig setUnit(TimeUnit unit) {
    this.unit = unit;
    return this;
  }

  /**
   * The time unit enum.
   */
  public enum TimeUnit {
    SECONDS,
    MINUTES,
    HOURS,
    DAYS
  }
}
