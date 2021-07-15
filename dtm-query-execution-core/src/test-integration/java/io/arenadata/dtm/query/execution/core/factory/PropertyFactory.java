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
package io.arenadata.dtm.query.execution.core.factory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.support.EncodedResource;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class PropertyFactory {

    static final YamlPropertySourceFactory factory = new YamlPropertySourceFactory();

    public static PropertySource<?> createPropertySource(String fileName) {
        try (InputStream inputStream = PropertyFactory.class.getClassLoader()
                .getResourceAsStream(fileName)) {
            assert inputStream != null;
            return factory.createPropertySource("core.properties",
                    new EncodedResource(new InputStreamResource(inputStream)));
        } catch (IOException e) {
            log.error("Error in reading properties file", e);
            throw new RuntimeException(e);
        }
    }
}
