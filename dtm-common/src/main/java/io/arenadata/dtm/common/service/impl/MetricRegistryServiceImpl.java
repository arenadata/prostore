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
package io.arenadata.dtm.common.service.impl;

import io.arenadata.dtm.common.model.DurationStatistic;
import io.arenadata.dtm.common.service.MetricRegistryService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MetricRegistryServiceImpl implements MetricRegistryService {

  private AtomicBoolean isModeOn = new AtomicBoolean(true);

  private ConcurrentHashMap<String, DurationStatistic> map = new ConcurrentHashMap<>();

  public void turnOff(){
    isModeOn.set(false);
  }
  public void turnOn(){
    isModeOn.set(true);
  }

  @Override
  public void append(String operation, Long duration) {
    if (isModeOn.get()) {
      DurationStatistic stats = map.computeIfAbsent(operation, DurationStatistic::new);
      stats.add(duration);
    }
  }

  @Override
  public <T> T measureTimeMillis(String operation, Supplier<T> block) {
    long start=0L;
    if(isModeOn.get()) {
      start = System.currentTimeMillis();
    }
    T result = block.get();
    if(isModeOn.get()) {
      append(operation, System.currentTimeMillis() - start);
    }
    return result;
  }

  @Override
  public String printSummaryStatistics() {
    if(isModeOn.get()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Статистика по операциям:\nОперация; Количество; Сумма; Макс; Мин; Среднее\n")
        .append(map.values().stream().map(DurationStatistic::toString).collect(Collectors.joining("\n")));
      return sb.toString();
    } else
      return "Измерение метрик отключено";
  }

}
