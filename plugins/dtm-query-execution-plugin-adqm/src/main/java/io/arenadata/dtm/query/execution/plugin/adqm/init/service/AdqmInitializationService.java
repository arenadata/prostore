/*
 * Copyright © 2021 ProStore
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
package io.arenadata.dtm.query.execution.plugin.adqm.init.service;

import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmInitializationService")
public class AdqmInitializationService implements PluginInitializationService {

    @Override
    public Future<Void> execute() {
        return Future.succeededFuture();
    }
}