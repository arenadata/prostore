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
package io.arenadata.dtm.query.calcite.core.util;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class RelNodeUtil {

    public static boolean isNeedToTrimSortColumns(RelRoot relRoot, RelNode sourceRelNode) {
        return sourceRelNode instanceof LogicalSort
                && sourceRelNode.getRowType().getFieldCount() != relRoot.validatedRowType.getFieldCount();
    }

    public static RelNode trimUnusedSortColumn(RelBuilder relBuilder, RelNode relNode, RelDataType validatedRowType) {
        relBuilder.clear();
        relBuilder.push(relNode);
        ImmutableList<RexNode> fields = relBuilder.fields(validatedRowType.getFieldNames());
        return relBuilder.project(fields).build();
    }
}
