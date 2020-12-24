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
package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.model.TableInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.arenadata.dtm.jdbc.util.DriverConstants.*;
import static io.arenadata.dtm.jdbc.util.DriverInfo.*;
import static org.apache.http.util.TextUtils.isEmpty;

public class DtmDatabaseMetaData implements DatabaseMetaData {

    private final BaseConnection connection;
    private ResultSet catalogs;

    public DtmDatabaseMetaData(BaseConnection dtmConnection) {
        this.connection = dtmConnection;
    }

    private List<String> getCatalogNames(ResultSet catalogs) throws SQLException {
        List<String> names = new ArrayList<>();
        while (catalogs.next()) {
            names.add(catalogs.getString(CATALOG_NAME_COLUMN));
        }
        return names;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<Field[]> result = Collections.singletonList(new Field[]{
                new Field(SCHEMA_NAME_COLUMN, ""),
                new Field(CATALOG_NAME_COLUMN, "")
        });
        return ((DtmStatement) this.connection.createStatement()).createDriverResultSet(result, getMetadata(result));
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (catalogs == null) {
            List<Field[]> result = connection.getQueryExecutor().getSchemas().stream()
                    .map(schemaInfo -> new Field[]{new Field(CATALOG_NAME_COLUMN, schemaInfo.getMnemonic())})
                    .collect(Collectors.toList());
            this.catalogs = ((DtmStatement) this.connection.createStatement()).createDriverResultSet(result, getMetadata(result));
        }
        return catalogs;
    }

    private List<ColumnMetadata> getMetadata(List<Field[]> result) {
        if (result.isEmpty()) {
            return Collections.emptyList();
        } else {
            Field[] fields = result.get(0);
            return Stream.of(fields)
                    .map(f -> new ColumnMetadata(f.getColumnLabel(), ColumnType.VARCHAR))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public ResultSet getTables(String catalog,
                               String schemaPattern,
                               String tableNamePattern,
                               String[] types) throws SQLException {
        List<String> catalogNames;
        if (isEmpty(catalog)) {
            catalogNames = getCatalogNames(getCatalogs());
        } else {
            catalogNames = Collections.singletonList(catalog);
        }
        final List<TableInfo> databaseTables = this.connection.getQueryExecutor().getTables(catalog);
        List<Field[]> result = catalogNames.stream()
                .flatMap(schemasName -> databaseTables.stream())
                .map(tableInfo -> new Field[]{
                        new Field(CATALOG_NAME_COLUMN, tableInfo.getDatamartMnemonic()),
                        new Field(SCHEMA_NAME_COLUMN, ""),
                        new Field(TABLE_NAME_COLUMN, tableInfo.getMnemonic()),
                        new Field(TABLE_TYPE_COLUMN, TABLE_TYPE),
                        new Field(REMARKS_COLUMN, ""),
                        new Field(SELF_REFERENCING_COL_NAME_COLUMN, null),
                        new Field(REF_GENERATION_COLUMN, null),
                        new Field(TABLE_OWNER_COLUMN, getUserName())
                }).collect(Collectors.toList());
        return ((DtmStatement) this.connection.createStatement()).createDriverResultSet(result, getMetadata(result));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<Field[]> result = Collections.singletonList(new Field[]{
                new Field(TABLE_TYPE_COLUMN, TABLE_TYPE),
                new Field(TABLE_TYPE_COLUMN, SYSTEM_VIEW_TYPE),
                new Field(TABLE_TYPE_COLUMN, VIEW_TYPE)
        });
        return ((DtmStatement) this.connection.createStatement()).createDriverResultSet(result, getMetadata(result));
    }

    @Override
    public ResultSet getColumns(String catalog,
                                String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        tableNamePattern = tableNamePattern.replace("\\", "");
        System.out.printf("Table name pattern: %s", tableNamePattern);
        //Временный костыль что бы пофиксить возможный запрос всех полей в схеме.
        if (tableNamePattern.indexOf('%') != -1) {
            ResultSet tables = this.getTables(catalog, schemaPattern, null, null);
            while (tables.next()) {
                final List<ColumnInfo> databaseColumns = this.connection.getQueryExecutor().getTableColumns(
                        tables.getString(CATALOG_NAME_COLUMN),
                        tables.getString(TABLE_NAME_COLUMN));
                System.out.printf("Table name: %s", tables.getString(TABLE_NAME_COLUMN));
                columns.addAll(databaseColumns);
            }
        } else {
            columns = this.connection.getQueryExecutor().getTableColumns(catalog, tableNamePattern);
        }
        this.connection.getCachedFieldMetadata().clear();
        this.connection.getCachedFieldMetadata().addAll(columns);
        List<Field[]> result = columns.stream()
                .map(columnInfo -> new Field[]{
                        new Field(CATALOG_NAME_COLUMN, columnInfo.getDatamartMnemonic()),
                        new Field(SCHEMA_NAME_COLUMN, null),
                        new Field(TABLE_NAME_COLUMN, columnInfo.getEntityMnemonic()),
                        new Field(COLUMN_NAME_COLUMN, columnInfo.getMnemonic()),
                        new Field(DATA_TYPE_COLUMN, columnInfo.getDataType().getSqlType()),
                        new Field(TYPE_NAME_COLUMN, columnInfo.getDataType().getAliases()[0].toUpperCase()),
                        new Field(COLUMN_SIZE_COLUMN, columnInfo.getLength()),
                        new Field(BUFFER_LENGTH_COLUMN, 0),
                        new Field(DECIMAL_DIGITS_COLUMN, columnInfo.getAccuracy()),
                        new Field(NUM_PREC_RADIX_COLUMN, null),
                        new Field(NULLABLE_COLUMN, isNullable(columnInfo) ? 1 : 0),
                        new Field(REMARKS_COLUMN, ""),
                        new Field(COLUMN_DEF_COLUMN, ""),
                        new Field(SQL_DATA_TYPE_COLUMN, 0),
                        new Field(SQL_DATETIME_SUB_COLUMN, 0),
                        new Field(CHAR_OCTET_LENGTH_COLUMN, null),
                        new Field(ORDINAL_POSITION_COLUMN, columnInfo.getOrdinalPosition()),
                        new Field(IS_NULLABLE_COLUMN, isNullable(columnInfo) ? "YES" : "NO"),
                        new Field(SCOPE_CATALOG_COLUMN, null),
                        new Field(SCOPE_SCHEMA_COLUMN, null),
                        new Field(SCOPE_TABLE_COLUMN, null),
                        new Field(SOURCE_DATA_TYPE_COLUMN, null),
                        new Field(IS_AUTOINCREMENT_COLUMN, "NO"),
                        new Field(IS_GENERATEDCOLUMN_COLUMN, "NO")
                }).collect(Collectors.toList());
        return ((DtmStatement) this.connection.createStatement()).createDriverResultSet(result, getMetadata(result));
    }

    private boolean isNullable(ColumnInfo columnInfo) {
        return columnInfo.getNullable() == null || columnInfo.getNullable();
    }

    @Override
    public String getDatabaseProductName() {
        return DATABASE_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() {
        return this.connection.getDBVersionNumber();
    }

    @Override
    public String getURL() {
        return this.connection.getUrl();
    }

    @Override
    public String getUserName() {
        return this.connection.getUserName();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog,
                                   String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog,
                                         String schemaPattern,
                                         String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog,
                                         String schema,
                                         String table,
                                         String columnNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(String catalog,
                                        String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog,
                                          String schema,
                                          String table,
                                          int scope,
                                          boolean nullable) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog,
                                       String parentSchema,
                                       String parentTable,
                                       String foreignCatalog,
                                       String foreignSchema,
                                       String foreignTable) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        //FIXME
        /* Реализованы 3 основных типа, как оказалось, для исправления в рамках DTM-98
        не потребовалось, но в последствии типы БД могут потребоваться, поэтому оставлю это здесь.
        String[] columnNames = new String[]{
                TYPE_NAME_COLUMN,
                DATA_TYPE_COLUMN,
                "PRECISION",
                "LITERAL_PREFIX",
                "LITERAL_SUFFIX",
                "CREATE_PARAMS",
                NULLABLE_COLUMN,
                "CASE_SENSITIVE",
                "SEARCHABLE",
                "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE",
                "AUTO_INCREMENT",
                "LOCAL_TYPE_NAME",
                "MINIMUM_SCALE",
                "MAXIMUM_SCALE",
                SQL_DATA_TYPE_COLUMN,
                SQL_DATETIME_SUB_COLUMN,
                NUM_PREC_RADIX_COLUMN};
        Object[][] data = new Object[][]{
                {"BIGINT", Types.BIGINT, 19, "", "", "", 1, false, 3, true, false, true, "BIGINT", 0, 0, 0, 0, 10},
                {"INT", Types.INTEGER, 10, "", "", "", 1, false, 3, true, false, true, "INT", 0, 0, 0, 0, 10},
                {"VARCHAR", Types.VARCHAR, 255, "'", "'", "(M)", 1, false, 3, false, false, false, "VARCHAR", 0, 0, 0, 0, 10}};
        List<Field[]> result = new ArrayList<>();
        for (int i=0; i<data.length; i++) {
            result.add(valueArrayToFieldArray(columnNames, data[i]));
        }
        return new DtmResultSet(connection, result);*/
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog,
                                  String schema,
                                  String table,
                                  boolean unique,
                                  boolean approximate) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return supportsResultSetType(type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return supportsResultSetType(type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return supportsResultSetType(type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog,
                             String schemaPattern,
                             String typeNamePattern,
                             int[] types) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public BaseConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getAttributes(String catalog,
                                   String schemaPattern,
                                   String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        final String[] dbVersionArr = this.connection.getDBVersionNumber().split("\\.");
        if (dbVersionArr.length > 0) {
            return Integer.parseInt(dbVersionArr[0]);
        } else {
            return 0;
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        final String[] dbVersionArr = this.connection.getDBVersionNumber().split("\\.");
        if (dbVersionArr.length > 1) {
            return Integer.parseInt(dbVersionArr[1]);
        } else {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return JDBC_MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }


    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getFunctions(String catalog,
                                  String schemaPattern,
                                  String functionNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog,
                                        String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public ResultSet getPseudoColumns(String catalog,
                                      String schemaPattern,
                                      String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
