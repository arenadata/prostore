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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.HISTORY_POSTFIX;

@Service("adgTruncateHistoryService")
public class AdgTruncateHistoryService implements TruncateHistoryService {
    private static final String SYS_CN_CONDITION_PATTERN = "\"sys_to\" < %s";
    private final AdgCartridgeClient adgCartridgeClient;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdgTruncateHistoryService(AdgCartridgeClient adgCartridgeClient,
                                     @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.adgCartridgeClient = adgCartridgeClient;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return getConditions(params)
                .compose(conditions -> params.getSysCn().isPresent()
                        ? deleteSpaceTuples(params, HISTORY_POSTFIX, conditions)
                        : deleteSpaceTuplesWithoutSysCn(params, conditions));
    }

    private Future<String> getConditions(TruncateHistoryParams params) {
        List<String> conditions = new ArrayList<>();
        params.getConditions()
                .map(val -> String.format("(%s)", val.toSqlString(sqlDialect)))
                .ifPresent(conditions::add);
        params.getSysCn()
                .map(sysCn -> String.format(SYS_CN_CONDITION_PATTERN, sysCn))
                .ifPresent(conditions::add);
        return Future.succeededFuture(String.join(" AND ", conditions));
    }

    private Future<Void> deleteSpaceTuples(TruncateHistoryParams params, String postfix, String conditions) {
        String spaceName = AdgUtils.getSpaceName(params.getEnv(), params.getEntity().getSchema(),
                params.getEntity().getName(), postfix);
        return adgCartridgeClient.deleteSpaceTuples(spaceName, conditions.isEmpty() ? null : conditions);
    }

    private Future<Void> deleteSpaceTuplesWithoutSysCn(TruncateHistoryParams params,
                                                    String conditions) {
        return Future.future(promise -> CompositeFuture.join(Arrays.asList(
                deleteSpaceTuples(params, ACTUAL_POSTFIX, conditions),
                deleteSpaceTuples(params, HISTORY_POSTFIX, conditions)
        ))
                .onSuccess(result -> promise.complete())
                .onFailure(promise::fail));
    }
}
