package io.arenadata.dtm.query.calcite.core.node;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@Slf4j
public class SqlSelectTree {
    public static final String IS_TABLE_OR_SNAPSHOTS_PATTERN = "(?i).*(^\\w+|JOIN|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)$";
    public static final String SELECT_AS_SNAPSHOT = "SNAPSHOT";
    private final Map<Integer, SqlTreeNode> nodeMap;
    private int idCounter;

    public SqlSelectTree(SqlNode sqlSelect) {
        nodeMap = new TreeMap<>();
        createRoot(sqlSelect).ifPresent(this::addNodes);
    }

    public SqlTreeNode getRoot() {
        return nodeMap.get(0);
    }

    public Optional<SqlTreeNode> getParentByChild(SqlTreeNode child) {
        return Optional.ofNullable(nodeMap.get(child.getParentId()));
    }

    public List<SqlTreeNode> findChildren(SqlTreeNode parent) {
        return nodeMap.values().stream()
                .filter(n -> n.getParentId() == parent.getId())
                .sorted()
                .collect(Collectors.toList());
    }

    public List<SqlTreeNode> findSnapshots() {
        return findNodesByPath(SELECT_AS_SNAPSHOT);
    }

    public List<SqlTreeNode> findNodesByPathRegex(String regex) {
        return filterChild(nodeMap.values().stream()
                .filter(n -> n.getKindPath().matches(regex))
                .collect(Collectors.toList()));
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
            flattenSqlIdentifier(treeNode, (SqlIdentifier) node);
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

    private void flattenSqlAlterView(SqlTreeNode parentTree, SqlCall parentNode) {
        addReflectNode(parentTree, parentNode, "name");
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, "columnList");
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, "query");
    }

    private void flattenSqlCreate(SqlTreeNode parentTree, SqlDdl parentNode) {
        flattenSqlDrop(parentTree, parentNode);
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, "columnList");
        parentTree.resetChildPos();
        addReflectNode(parentTree, parentNode, "query");
    }

    private void addReflectNode(SqlTreeNode parentTree, SqlNode parentNode, String fieldName) {
        parentTree.createChild(idCounter++,
                readNode(parentNode, fieldName),
                node -> writeNode(parentNode, node, fieldName))
                .ifPresent(this::addNodes);
    }

    private void flattenSqlDrop(SqlTreeNode parentTree, SqlDdl parentNode) {
        addReflectNode(parentTree, parentNode, "name");
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
            parentTree.createChild(idCounter++, itemNode, n -> parentNode.setOperand(finalI, n))
                    .ifPresent(this::addNodes);
        }
    }

    private void flattenSqlUpdate(SqlTreeNode parentTree, SqlNode parentNode) {
        addReflectNode(parentTree, parentNode, "query");
        addReflectNode(parentTree, parentNode, "query");
        addReflectNode(parentTree, parentNode, "query");
        addReflectNode(parentTree, parentNode, "query");
    }

    private void flattenSqlBasicCall(SqlTreeNode parentTree, SqlBasicCall parentNode) {
        flattenSqlCall(parentTree, parentNode);
    }

    private void flattenSqlSnapshot(SqlTreeNode parent, SqlSnapshot parentNode) {
        flattenSqlCall(parent, parentNode);
    }

    private void flattenSqlIdentifier(SqlTreeNode parentTree, SqlIdentifier parentNode) {
        parentTree.setIdentifier(true);
    }

    private void flattenSqlCall(SqlTreeNode parentTree, SqlCall parentNode) {
        val nodes = parentNode.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(idCounter++, itemNode, n -> parentNode.setOperand(finalI, n))
                    .ifPresent(this::addNodes);
        }
    }

    private void flattenSqlSelect(SqlTreeNode parentTree, SqlSelect parentNode) {
        parentTree.createChild(idCounter++,
                parentNode.getSelectList(),
                sqlNode -> parentNode.setSelectList((SqlNodeList) sqlNode))
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(idCounter++,
                parentNode.getFrom(),
                parentNode::setFrom)
                .ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(idCounter++, parentNode.getWhere(), parentNode::setWhere)
                .ifPresent(this::addNodes);
    }

    private void flattenSqlJoin(SqlTreeNode parentTree, SqlJoin parentNode) {
        parentTree.resetChildPos();
        parentTree.createChild(idCounter++, parentNode.getLeft(), parentNode::setLeft).ifPresent(this::addNodes);
        parentTree.resetChildPos();
        parentTree.createChild(idCounter++, parentNode.getRight(), parentNode::setRight).ifPresent(this::addNodes);
    }

    private void flattenSqlNodeList(SqlTreeNode parentTree, SqlNodeList parentNode) {
        val nodes = parentNode.getList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(idCounter++, itemNode, n -> parentNode.set(finalI, n)).ifPresent(this::addNodes);
        }
    }

    public List<SqlTreeNode> findAllTableAndSnapshots() {
        return this.findNodesByPathRegex(IS_TABLE_OR_SNAPSHOTS_PATTERN).stream()
                .sorted()
                .collect(Collectors.toList());
    }

    private void addNodes(SqlTreeNode... nodes) {
        for (val node : nodes) {
            nodeMap.put(node.getId(), node);
            flattenSql(node);
        }
    }

    private Optional<SqlTreeNode> createRoot(SqlNode node) {
        return Optional.of(new SqlTreeNode(idCounter++,
                -1,
                0,
                node,
                null,
                node.getKind().toString()));
    }
}
