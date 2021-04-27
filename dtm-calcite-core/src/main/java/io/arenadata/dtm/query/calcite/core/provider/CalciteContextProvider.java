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
package io.arenadata.dtm.query.calcite.core.provider;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public abstract class CalciteContextProvider {
    protected final List<RelTraitDef> traitDefs;
    protected final RuleSet prepareRules;
    protected final SqlParser.Config configParser;
    protected final CalciteSchemaFactory calciteSchemaFactory;

    public CalciteContextProvider(SqlParser.Config configParser,
                                  CalciteSchemaFactory calciteSchemaFactory) {
        this.configParser = configParser;
        prepareRules =
                RuleSets.ofList(
                        EnumerableRules.ENUMERABLE_RULES);

        traitDefs = new ArrayList<>();
        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        this.calciteSchemaFactory = calciteSchemaFactory;
    }

    public CalciteContext context(List<Datamart> schemas) {
        final SchemaPlus rootSchema = DtmCalciteFramework.createRootSchema(true);
        Datamart defaultDatamart = null;
        if (schemas != null) {
            Optional<Datamart> defaultSchemaOptional = schemas.stream()
                    .filter(Datamart::getIsDefault)
                    .findFirst();
            if (defaultSchemaOptional.isPresent()) {
                defaultDatamart = defaultSchemaOptional.get();
            }
            schemas.stream()
                    .filter(d -> !d.getIsDefault())
                    .forEach(d -> calciteSchemaFactory.addSchema(rootSchema, d));
        }

        final SchemaPlus defaultSchema = defaultDatamart == null ?
                rootSchema : calciteSchemaFactory.addSchema(rootSchema, defaultDatamart);

        FrameworkConfig config = DtmCalciteFramework.newConfigBuilder()
                .parserConfig(configParser)
                .defaultSchema(defaultSchema)
                .traitDefs(traitDefs).programs(Programs.of(prepareRules))
                .sqlToRelConverterConfig(SqlToRelConverter.configBuilder().withExpand(false).build())
                .build();
        Planner planner = DtmCalciteFramework.getPlanner(config);
        return new CalciteContext(rootSchema, planner, RelBuilder.create(config));
    }

    public void enrichContext(CalciteContext context, List<Datamart> schemas) {
        schemas.forEach(s -> calciteSchemaFactory.addSchema(context.getSchema(), s));
    }
}
