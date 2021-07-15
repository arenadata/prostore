## Ядро выполнения запроса (dtm-query-execution-core)

## Запуск плагина на на примере ADB (dtm-query-execution-adb)

Запустить через IDEA, указав main-класс:
`io.arenadata.dtm.query.execution.core.ServiceQueryExecutionApplication`

VM Options добавить:
`-Dspring.config.location=classpath:application.yml,classpath:config/plugin/adb/application.yml`

Для плагина ADG (dtm-query-execution-adg) добавить:
`classpath:config/plugin/adg/application.yml`
