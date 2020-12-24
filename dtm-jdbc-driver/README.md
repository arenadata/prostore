# DTM JDBC Driver

JDBC драйвер для подключения к витрине.

### Подключение через SQL-клиенты

Класс драйвера  ``io.arenadata.dtm.jdbc.DtmDriver``

Строка подключения : ``jdbc:adtm://{host}:{port}/{datamartMnemonics}``

``host`` - ip адрес, где развернут LL-R

``port`` - порт, на котором развернут LL-R

``datamartMnemonics`` - название подключаемой витрины. **Не обязательный атрибут*
