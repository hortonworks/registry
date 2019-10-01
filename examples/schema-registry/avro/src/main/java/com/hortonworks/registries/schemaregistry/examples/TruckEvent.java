/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.examples;

import java.io.Serializable;

/**
 *
 */
public class TruckEvent implements Serializable {
    Long driverId;
    Long truckId;
    String eventTime;
    String eventType;
    Double longitude;
    Double latitude;
    String eventKey;
    String correlationId;
    String driverName;
    Long routeId;
    String routeName;
    String eventDate;
    Long miles;

    public TruckEvent() {
    }

    public Long getDriverId() {
        return driverId;
    }

    public Long getTruckId() {
        return truckId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public String getEventKey() {
        return eventKey;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getDriverName() {
        return driverName;
    }

    public Long getRouteId() {
        return routeId;
    }

    public String getRouteName() {
        return routeName;
    }

    public String getEventDate() {
        return eventDate;
    }

    public Long getMiles() {
        return miles;
    }

    public void setMiles(Long miles) {
        this.miles = miles;
    }
}
