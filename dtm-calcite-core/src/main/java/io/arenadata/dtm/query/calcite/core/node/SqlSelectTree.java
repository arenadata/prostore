package io.arenadata.dtm.query.calcite.core.node;

import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@Slf4j
public class SqlSelectTree {
    public static final String IS_TABLE_OR_SNAPSHOTS_PATTERN = "(?i).*(^\\w+|JOIN(|\\[\\d+\\])|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)$";
    public static final String IS_TABLE_OR_SNAPSHOTS_WITH_CHILD_PATTERN = "(?i).*(^\\w+|JOIN(|\\[\\d+\\])|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)(|\\.IDENTIFIER)$";
    public static final String SELECT_AS_SNAPSHOT = "SNAPSHOT";
    private static final String QUERY_FIELD = "query";
    private static final String COLUMN_LIST_FIELD = "columnList";
    private static final String NAME_FIELD = "name";
    private final Map<Integer, SqlTreeNode> nodeMap;
    private final Map<Integer, List<SqlTreeNode>> childNodesMap;
    private int idCounter;

    public SqlSelectTree(SqlNode sqlSelect) {
        nodeMap = new HashMap<>();
        childNodesMap = new HashMap<>();
        createRoot(sqlSelect).ifPresent(this::addNodes);
    }

    SqlSelectTree(Map<Integer, SqlTreeNode> nodeMap, Map<Integer, List<SqlTreeNode>> childNodesMap, int idCounter) {
        this.idCounter = idCounter;
        this.nodeMap = nodeMap;
        this.childNodesMap = childNodesMap;
    }

    public SqlTreeNode getRoot() {
        return nodeMap.get(0);
    }

    public Optional<SqlTreeNode> getParentByChild(SqlTreeNode child) {
        return getParentByChild(nodeMap, child);
    }

    private Optional<SqlTreeNode> getParentByChild(Map<Integer, SqlTreeNode> nodeMap, SqlTreeNode child) {
        return Optional.ofNullable(nodeMap.get(child.getParentId()));
    }

    public List<SqlTreeNode> findSnapshots() {
        return findNodesByPath(SELECT_AS_SNAPSHOT);
    }

    public List<SqlTreeNode> findNodesByPathRegex(String regex) {
        return filterChild(findAllNodesByPathRegex(regex));
    }

    public List<SqlTreeNode> findNodesByPath(String pathPostfix) {
        return filterChild(nodeMap.values().stream()
                .filter(n -> n.getKindPath().endsWith(pathPostfix))
                .collect(Collectors.toList()));
    }

    public List<SqlTreeNode> findNodes(Predicate<SqlTreeNode> predicate) {
        return filterChild(nodeMap.values().stream()
                .filter(predicate)
                .collect(Collectors.toList()));
    }

    public List<SqlTreeNode> findNodesByParent(SqlTreeNode sqlTreeNode) {
        return childNodesMap.getOrDefault(sqlTreeNode.getId(), Collections.emptyList());
    }

    private List<SqlTreeNode> findAllNodesByPathRegex(String regex) {
        return nodeMap.values().stream()
                .filter(n -> n.getKindPath().matches(regex))
                .collect(Collectors.toList());
    }

    private List<SqlTreeNode> filterChild(List<SqlTreeNode> nodeList) {
        return nodeList.stream()
                .filter(n1 -> nodeList.stream().noneMatch(n2 -> n1.getParentId() == n2.getId()))
                .sorted()
                .collect(Collectors.toList());
    }

    private void flattenSql(SqlTreeNode treeNode) {
        val node = treeNode.getNode();
        if (node instanceof SqlSelect) {
            flattenSqlSelect(treeNode, (SqlSelect) node);
        } else if (node instanceof SqlNodeList) {
            flattenSqlNodeList(treeNode, (SqlNodeList) node);
        } else if (node instanceof SqlJoin) {
            flattenSqlJoin(treeNode, (SqlJoin) node);
        } else if (node instanceof SqlIdentifier) {
            flattenSqlIdentifier(treeNode);
        } else if (node instanceof LimitableSqlOrderBy) {
            flattenLimitableSqlOrderBy(treeNode, (LimitableSqlOrderBy) node);
        } else if (node instanceof SqlSnapshot) {
            flattenSqlSnapshot(treeNode, (SqlSnapshot) node);
        } else if (node instanceof SqlBasicCall) {
            flattenSqlBasicCall(treeNode, (SqlBasicCall) node);
        } else if (node instanceof SqlInsert) {
            flattenSqlInsert(treeNode, (SqlCall) node);
        } else if (node instanceof SqlCreate) {
            flattenSqlCreate(treeNode, (SqlDdl) node);
        } else if (node instanceof SqlDrop) {
            flattenSqlDrop(treeNode, (SqlDdl) node);
        } else if (node instanceof SqlAlter) {
            flattenSqlAlterView(treeNode, (SqlCall) node);
        } else if (node instanceof SqlCall) {
            flattenSqlCall(treeNode, (SqlCall) node);
        }
    }

    private void flattenLimitableSqlOrderBy(SqlTreeNode treeNode, LimitableSqlOrderBy node) {
        flattenSqlCall(treeNode, node);
    }

    private void flattenSqlAlterView(SqlTreeNode parentTree, SqlCall parentNode) {
        addReflectNode(parentTree, parentNode, NAME_FIELD);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, COLUMN_LIST_FIELD);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, QUERY_FIELD);
    }

    private void flattenSqlCreate(SqlTreeNode parentTree, SqlDdl parentNode) {
        flattenSqlDrop(parentTree, parentNode);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, COLUMN_LIST_FIELD);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, QUERY_FIELD);
    }

    private void addReflectNode(SqlTreeNode parentTree, SqlNode parentNode, String fieldName) {
        parentTree.createChild(getNextId(),
                readNode(parentNode, fieldName),
                new SqlNodeConsumer<>(parentNode, (parent, child) -> writeNode(parent, child, fieldName)))
                .ifPresent(this::addNodes);
    }

    private void flattenSqlDrop(SqlTreeNode parentTree, SqlDdl parentNode) {
        addReflectNode(parentTree, parentNode, NAME_FIELD);
    }

    @SneakyThrows
    private SqlNode readNode(SqlNode o, String fieldName) {
        try {
            return (SqlNode) FieldUtils.readField(o, fieldName, true);
        } catch (Exception ex) {
            return null;
        }
    }

    @SneakyThrows
    private void writeNode(SqlNode parent, SqlNode child, String fieldName) {
        FieldUtils.writeField(parent, fieldName, child, true);
    }

    private void flattenSqlInsert(SqlTreeNode parentTree, SqlCall parentNode) {
        val nodes = parentNode.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.resetChildPos();
            parentTree.createChild(getNextId(),
                    itemNode,
                    new SqlNodeConsumer<>(parentNode, (sqlCall, sqlNode) -> sqlCall.setOperand(finalI, sqlNode)))
                    .ifPresent(this::addNodes);
        }
    }

    private void flattenSqlBasicCall(SqlTreeNode parentTree, SqlBasicCall parentNode) {
        flattenSqlCall(parentTree, parentNode);
    }

    private void flattenSqlSnapshot(SqlTreeNode parent, SqlSnapshot parentNode) {
        flattenSqlCall(parent, parentNode);
    }

    private void flattenSqlIdentifier(SqlTreeNode parentTree) {
        parentTree.setIdentifier(true);
    }

    private void flattenSqlCall(SqlTreeNode parentTree, SqlCall parentNode) {
        val nodes = parentNode.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(getNextId(),
                    itemNode,
                    new SqlNodeConsumer<>(parentNode, (sqlCall, sqlNode) -> sqlCall.setOperand(finalI, sqlNode)))
                    .ifPresent(this::addNodes);
        }
    }

    private void flattenSqlSelect(SqlTreeNode parentTree, SqlSelect parentNode) {
        parentTree.createChild(getNextId(),
                parentNode.getSelectList(),
                new SqlNodeConsumer<>(parentNode, (sqlSelect, sqlNode) -> sqlSelect.setSelectList((SqlNodeList) sqlNode)))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(getNextId(), parentNode.getWhere(), new SqlNodeConsumer<>(parentNode, SqlSelect::setWhere))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(getNextId(),
                parentNode.getFrom(),
                new SqlNodeConsumer<>(parentNode, SqlSelect::setFrom))
                .ifPresent(this::addNodes);
    }

    private void flattenSqlJoin(SqlTreeNode parentTree, SqlJoin parentNode) {
        flattenSqlCall(parentTree, parentNode);
    }

    private void flattenSqlNodeList(SqlTreeNode parentTree, SqlNodeList parentNode) {
        val nodes = parentNode.getList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(getNextId(),
                    itemNode,
                    new SqlNodeConsumer<>(parentNode, (sqlNodes, sqlNode) -> sqlNodes.set(finalI, sqlNode)))
                    .ifPresent(this::addNodes);
        }
    }

    public List<SqlTreeNode> findAllTableAndSnapshots() {
        return this.findNodesByPathRegex(IS_TABLE_OR_SNAPSHOTS_PATTERN).stream()
                .sorted()
                .collect(Collectors.toList());
    }

    public List<SqlTreeNode> findAllTableAndSnapshotWithChildren() {
        return this.findAllNodesByPathRegex(IS_TABLE_OR_SNAPSHOTS_WITH_CHILD_PATTERN).stream()
                .sorted()
                .collect(Collectors.toList());
    }

    private void addNodes(SqlTreeNode... nodes) {
        for (val node : nodes) {
            nodeMap.put(node.getId(), node);
            childNodesMap.computeIfAbsent(node.getParentId(), i -> new ArrayList<>()).add(node);
            flattenSql(node);
        }
    }

    private Optional<SqlTreeNode> createRoot(SqlNode node) {
        return Optional.of(new SqlTreeNode(getNextId(),
                -1,
                0,
                node,
                null,
                node.getKind().toString()));
    }

    private int getNextId() {
        return idCounter++;
    }

    public SqlSelectTree copy() {
        val copiedSqlNodeMap = nodeMap.entrySet().stream()
                .map(this::getNodePairWithCopiedSqlNode)
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        val resultNodeMap = copiedSqlNodeMap.entrySet().stream()
                .map(e -> getNodePairWithNewNodeSetter(copiedSqlNodeMap, e))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return new SqlSelectTree(resultNodeMap, childNodesMap, idCounter);
    }

    private Pair<Integer, SqlTreeNode> getNodePairWithCopiedSqlNode(Map.Entry<Integer, SqlTreeNode> e) {
        SqlNode origNode = e.getValue().getNode();
        SqlNode clonedNode;
        if (origNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) origNode;
            clonedNode = sqlBasicCall.getOperator().createCall(sqlBasicCall.getFunctionQuantifier(),
                    sqlBasicCall.getParserPosition(),
                    Arrays.copyOf(sqlBasicCall.operands, sqlBasicCall.operands.length));
        } else {
            clonedNode = SqlNode.clone(origNode);
        }
        return Pair.of(e.getKey(),
                e.getValue().toBuilder()
                        .node(clonedNode)
                        .build());
    }

    private Pair<Integer, SqlTreeNode> getNodePairWithNewNodeSetter(Map<Integer, SqlTreeNode> sqlNodeMap,
                                                                    Map.Entry<Integer, SqlTreeNode> e) {
        SqlTreeNode origTreeNode = e.getValue();
        val parentOpt = getParentByChild(sqlNodeMap, origTreeNode);
        if (parentOpt.isPresent()) {
            SqlNode parentNode = parentOpt.get().getNode();
            val nwSqlNodeSetter = origTreeNode.getSqlNodeSetter().toBuilder()
                    .parentNode(parentNode)
                    .build();
            val clonedTreeNode = origTreeNode.toBuilder()
                    .sqlNodeSetter(nwSqlNodeSetter)
                    .build();

            clonedTreeNode.getSqlNodeSetter().accept(clonedTreeNode.getNode());
            return Pair.of(e.getKey(), clonedTreeNode);
        } else {
            return Pair.of(e.getKey(), e.getValue());
        }
    }
}
