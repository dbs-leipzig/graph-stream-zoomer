/*
 * Copyright © 2021 - 2024 Leipzig University (Database Research Group)
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
package edu.dbsleipzig.stream.grouping.application.functions;

import org.apache.flink.api.java.tuple.Tuple15;

/**
 * A tuple class for readability reasons.
 */
public class CitibikeTuple15 extends Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> {

  public CitibikeTuple15() {
  }

  public CitibikeTuple15(String tripDuration, String startTime, String stopTime, String startStationId,
    String startStationName, String startStationLat, String startStationLong, String stopStationId,
    String stopStationName, String stopStationLat, String stopStationLong, String bikeId, String userType,
    String birthYear, String gender) {
    super(tripDuration, startTime, stopTime, startStationId, startStationName, startStationLat,
      startStationLong,stopStationId,stopStationName, stopStationLat, stopStationLong, bikeId, userType,
      birthYear, gender);
  }
}
