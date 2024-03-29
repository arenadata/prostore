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
package io.arenadata.dtm.query.execution.core.calcite.configuration;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.impl.DeltaInformationExtractorImpl;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreQueryTemplateExtractor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.ConversionUtil;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.Charset;

@Configuration
public class CalciteConfiguration {
    private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

    static {
        System.setProperty("saffron.default.charset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.nationalcharset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.collation.name", String.format("%s$en_US", DEFAULT_CHARSET.name()));
    }

    @Bean("coreParser")
    public SqlParserImplFactory getSqlParserFactory() {
        return new CalciteCoreConfiguration().eddlParserImplFactory();
    }

    @Bean("coreParserConfig")
    public SqlParser.Config configEddlParser(@Qualifier("coreParser") SqlParserImplFactory factory) {
        return SqlParser.configBuilder()
                .setParserFactory(factory)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }

    @Bean
    public DeltaInformationExtractor deltaInformationExtractor() {
        return new DeltaInformationExtractorImpl();
    }

    @Bean("coreSqlDialect")
    public SqlDialect coreSqlDialect() {
        return new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    }

    @Bean("coreQueryTmplateExtractor")
    public QueryTemplateExtractor queryTemplateExtractor(@Qualifier("coreCalciteDefinitionService")
                                                                 DefinitionService<SqlNode> definitionService,
                                                         @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        return new CoreQueryTemplateExtractor(definitionService, sqlDialect);
    }

    @Bean("coreRelToSqlConverter")
    public DtmRelToSqlConverter dtmRelToSqlConverter(@Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        return new DtmRelToSqlConverter(sqlDialect);
    }
}
