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
package io.arenadata.dtm.common.calcite;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;

public class CalciteContext {
  private SchemaPlus schema;
  private Planner planner;
  private RelBuilder relBuilder;

  public CalciteContext(SchemaPlus schema, Planner planner, RelBuilder relBuilder) {
    this.schema = schema;
    this.planner = planner;
    this.relBuilder = relBuilder;
  }

  public CalciteContext(SchemaPlus schema, Planner planner) {
    this.schema = schema;
    this.planner = planner;
  }

  public SchemaPlus getSchema() {
    return schema;
  }

  public Planner getPlanner() {
    return planner;
  }

  public void setSchema(SchemaPlus schema) {
    this.schema = schema;
  }

  public RelBuilder getRelBuilder() {
    return relBuilder;
  }
}
