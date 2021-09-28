package io.arenadata.dtm.query.calcite.core.node;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class SqlKindKey {
    private final SqlKind sqlKind;
    private final Integer childNum;
}
