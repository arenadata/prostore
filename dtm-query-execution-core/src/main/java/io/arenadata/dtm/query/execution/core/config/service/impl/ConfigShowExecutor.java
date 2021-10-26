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
package io.arenadata.dtm.query.execution.core.config.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.calcite.core.extension.config.function.SqlConfigShow;
import io.arenadata.dtm.query.execution.core.config.dto.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.config.service.ConfigExecutor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

@Slf4j
@Component
public class ConfigShowExecutor implements ConfigExecutor {
    private static final Pattern ENVIRONMENT_PATTERN = Pattern.compile("\\$\\{(\\w+):.*}");
    public static final String PARAMETER_NAME_COLUMN = "parameter_name";
    public static final String PARAMETER_VALUE_COLUMN = "parameter_value";
    public static final String ENVIRONMENT_VARIABLE_COLUMN = "environment_variable";
    private static final List<ColumnMetadata> METADATA = Arrays.asList(
            ColumnMetadata.builder().name(PARAMETER_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(PARAMETER_VALUE_COLUMN).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(ENVIRONMENT_VARIABLE_COLUMN).type(ColumnType.VARCHAR).build()
    );
    private final ConfigurableEnvironment environment;

    public ConfigShowExecutor(ConfigurableEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        return Future.future(event -> {
            val sqlNode = (SqlConfigShow) context.getSqlNode();
            val properties = getProperties();
            val result = getAllOrSelected(properties, sqlNode.getParameterName());

            event.complete(QueryResult.builder()
                    .result(result.stream().map(Property::toMap).collect(Collectors.toList()))
                    .metadata(METADATA)
                    .build());
        });
    }

    private List<Property> getAllOrSelected(List<Property> properties, SqlCharStringLiteral parameterName) {
        if (parameterName == null) {
            return properties;
        }

        val parameterToFind = parameterName.getNlsString().getValue();
        val result = properties.stream()
                .filter(property -> parameterToFind.equalsIgnoreCase(property.parameterName) || parameterToFind.equalsIgnoreCase(property.environmentName))
                .findFirst();
        return result.map(Collections::singletonList).orElse(emptyList());

    }

    private List<Property> getProperties() {
        List<Property> result = new ArrayList<>();
        for (PropertySource<?> propertySource : environment.getPropertySources()) {
            if (propertySource instanceof OriginTrackedMapPropertySource) {
                ((OriginTrackedMapPropertySource) propertySource).getSource().forEach((key, value) -> {
                    result.add(convertToProperty(key, value.toString()));
                });
            }
        }
        return result;
    }

    private Property convertToProperty(String key, String value) {
        val parameterName = key.replace(".", ":");
        val environmentName = extractEnvironmentVariable(value);
        val resolved = resolveProperty(key);
        return new Property(parameterName, environmentName, resolved);
    }

    private String extractEnvironmentVariable(String value) {
        val matcher = ENVIRONMENT_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }

        return matcher.group(1);
    }

    private String resolveProperty(String o) {
        return environment.getProperty(o);
    }

    @Override
    public SqlConfigType getConfigType() {
        return SqlConfigType.CONFIG_SHOW;
    }

    @AllArgsConstructor
    private static class Property {
        private final String parameterName;
        private final String environmentName;
        private final String resolvedValue;

        private Map<String, Object> toMap() {
            Map<String, Object> result = new HashMap<>();
            result.put(PARAMETER_NAME_COLUMN, parameterName);
            result.put(PARAMETER_VALUE_COLUMN, resolvedValue);
            result.put(ENVIRONMENT_VARIABLE_COLUMN, environmentName);
            return result;
        }
    }

}
