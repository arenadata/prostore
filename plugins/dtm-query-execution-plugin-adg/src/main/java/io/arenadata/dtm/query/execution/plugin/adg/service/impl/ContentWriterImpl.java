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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.config.ConsumerConfig;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.config.TopicsConfig;
import io.arenadata.dtm.query.execution.plugin.adg.service.ContentWriter;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;

@Service
public class ContentWriterImpl implements ContentWriter {

  private ObjectMapper yamlMapper;

  @Autowired
  public ContentWriterImpl(@Qualifier("yamlMapper") ObjectMapper yamlMapper) {
    this.yamlMapper = yamlMapper;
  }

  @SneakyThrows
  @Override
  public String toContent(Object config) {
    return yamlMapper.writeValueAsString(config);
  }

  @SneakyThrows
  @Override
  public ConsumerConfig toConsumerConfig(String content) {
    return yamlMapper.readValue(content, new TypeReference<ConsumerConfig>() {});
  }

  @SneakyThrows
  @Override
  public LinkedHashMap<String, TopicsConfig> toTopicsConfig(String content) {
    return yamlMapper.readValue(content, new TypeReference<LinkedHashMap<String, TopicsConfig>>() {
    });
  }
}
