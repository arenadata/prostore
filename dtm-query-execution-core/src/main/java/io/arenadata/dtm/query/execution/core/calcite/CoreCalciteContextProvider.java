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
package io.arenadata.dtm.query.execution.core.calcite;

import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component("coreCalciteContextProvider")
public class CoreCalciteContextProvider extends CalciteContextProvider {
    public CoreCalciteContextProvider(@Qualifier("coreParserConfig") SqlParser.Config configParser,
                                      @Qualifier("coreCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
