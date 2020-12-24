/*
 * Copyright © 2020 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.service.DeltaService;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtUploadDataKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.ResConfig;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
@ExtendWith(VertxExtension.class)
class AdgCartridgeClientImplIT {
  @MockBean
  private DeltaService deltaService;

  @Autowired
  private AdgCartridgeClient client;

  @Test
  void getFiles(VertxTestContext testContext) throws Throwable {
    client.getFiles(ar -> {
      if (ar.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  @Test
  void setFiles(VertxTestContext testContext) throws Throwable {
    client.getFiles(ar -> {
      if (ar.succeeded()) {
        List<OperationFile> files = ar.result().getData().getCluster().getConfig()
          .stream().map(ResConfig::toOperationFile).collect(Collectors.toList());
        client.setFiles(files, ar2 -> {
          if (ar2.succeeded()) {
            testContext.completeNow();
          } else {
            testContext.failNow(ar2.cause());
          }
        });
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  @Test
  void getSchema(VertxTestContext testContext) throws Throwable {
    client.getSchema(ar -> {
      if (ar.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  @Test
  @SneakyThrows
  void setSchema(VertxTestContext testContext) {
    client.getSchema(ar -> {
      if (ar.succeeded()) {
        val yaml = ar.result().getData().getCluster().getSchema().getYaml();
        client.setSchema(yaml, ar2 -> {
          if (ar2.succeeded()) {
            testContext.completeNow();
          } else {
            testContext.failNow(ar.cause());
          }
        });
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  @Test
  @SneakyThrows
  void uploadData(VertxTestContext testContext) {
    val request = new TtUploadDataKafkaRequest("select count(*) from employees",
            "test", 1000, new JsonObject(""));

    client.uploadData(request, ar -> {
      if (ar.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  @Test
  @SneakyThrows
  void badUploadData(VertxTestContext testContext) {
    val request = new TtUploadDataKafkaRequest("count(*) from employees",
            "test", 1000, new JsonObject(""));
    client.uploadData(request,ar -> {
      if (ar.succeeded()) {
        testContext.failNow(ar.cause());
      } else {
        if ("ADG_OUTPUT_PROCESSOR_003: ERROR: Only select queries allowed".equals(ar.cause().getMessage())) {
          testContext.completeNow();
        } else {
          testContext.failNow(ar.cause());
        }
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

}
