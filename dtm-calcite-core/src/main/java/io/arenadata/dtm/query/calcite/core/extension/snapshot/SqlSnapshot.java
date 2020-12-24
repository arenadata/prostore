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
package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

public class SqlSnapshot extends org.apache.calcite.sql.SqlSnapshot {

    public static final String AS_OF = "AS OF";
    private final SnapshotOperator snapshotOperator;
    private SqlNode tableRef;
    private SqlNode period;
    private String deltaDateTime;
    private Boolean isLatestUncommittedDelta;
    private SelectOnInterval startedInterval;
    private SelectOnInterval finishedInterval;
    private Long deltaNum;
    private SnapshotDeltaIntervalOperator startedOperator;
    private SnapshotDeltaIntervalOperator finishedOperator;
    private SnapshotDeltaNumOperator deltaNumOperator;
    private SnapshotLatestUncommittedDeltaOperator latestUncommittedDeltaOperator;

    public SqlSnapshot(SqlParserPos pos, SqlNode tableRef, SqlNode period, SqlOperator started,
                       SqlOperator finished, SqlNode num, SqlLiteral isLatestUncommittedDelta) {
        super(pos, tableRef, period);
        this.tableRef = (SqlNode) Objects.requireNonNull(tableRef);
        this.period = (SqlNode) period;
        this.snapshotOperator = new SnapshotOperator();
        this.startedOperator = new SnapshotStartedOperator(pos, this.period, started);
        this.finishedOperator = new SnapshotFinishedOperator(pos, this.period, finished);
        this.deltaNumOperator = new SnapshotDeltaNumOperator(pos, (SqlNumericLiteral) num);
        this.latestUncommittedDeltaOperator = new SnapshotLatestUncommittedDeltaOperator(pos, isLatestUncommittedDelta);
        initSnapshotAttributes();
    }

    @Override
    public SqlOperator getOperator() {
        return this.snapshotOperator;
    }

    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.tableRef = (SqlNode) Objects.requireNonNull(operand);
                break;
            case 1:
                this.period = (SqlNode) Objects.requireNonNull(operand);
                initSnapshotAttributes();
                break;
            default:
                throw new AssertionError(i);
        }
    }

    private void initSnapshotAttributes() {
        this.startedInterval = this.startedOperator.getDeltaInterval();
        this.finishedInterval = this.finishedOperator.getDeltaInterval();
        this.deltaNum = this.deltaNumOperator.getDeltaNum();
        this.isLatestUncommittedDelta = this.latestUncommittedDeltaOperator.getIsLatestUncommittedDelta();
        this.deltaDateTime = createDeltaDateTime();
    }

    private String createDeltaDateTime() {
        if (this.deltaNum != null ||
                this.startedInterval != null || this.finishedInterval != null || this.isLatestUncommittedDelta) {
            return null;
        } else {
            return this.period.toString();
        }
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.snapshotOperator.unparse(writer, this, 0, rightPrec);
        if (this.getStartedInterval() == null && this.getFinishedInterval() == null) {
            writer.keyword(AS_OF);
        }
        this.deltaNumOperator.unparse(writer, 0, 0);
        this.startedOperator.unparse(writer, 0, 0);
        this.finishedOperator.unparse(writer, 0, 0);
        this.latestUncommittedDeltaOperator.unparse(writer, 0, 0);
        if (this.getDeltaDateTime() != null) {
            this.period.unparse(writer, 0, 0);
        }
    }

    public SqlNode getTableRef() {
        return this.tableRef;
    }

    public SelectOnInterval getStartedInterval() {
        return startedInterval;
    }

    public SelectOnInterval getFinishedInterval() {
        return finishedInterval;
    }

    public String getDeltaDateTime() {
        return deltaDateTime;
    }

    public Boolean getLatestUncommittedDelta() {
        return isLatestUncommittedDelta;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }
}
