package com.hortonworks.registries.webservice.healthchecks;

import com.codahale.metrics.health.HealthCheck;

public class DummyHealthCheck extends HealthCheck {

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
