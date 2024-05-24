# Arrow Utils
Arrow Utils is an open-source utility library that offers convenient ways to convert various data types into the Apache Arrow format. This project aims to simplify data format conversion involved in data processing, transfer, and storage procedures implemented in Java. Whether the data originates from plain Java POJOs, JDBC-acquired datasets, or other data structures, Arrow Utils helps you to effortlessly convert them into the high-performance Apache Arrow data format.

## Features
- **POJO to Arrow Format Conversion**: Easily convert your Java Plain Old Java Objects (POJOs) to Arrow data structures.
- **JDBC Result Set to Arrow Format Conversion**: Allows you to directly convert data from JDBC result sets (currently supporting SQLite syntax) to Arrow format, efficiently utilizing Arrow's memory efficiency and speed.
- **Support for Multiple Data Sources**: In addition to POJOs and JDBC result sets, future support may be added for other data sources such as CSV, JSON, Parquet, and more.
- **Lightweight**: Minimize dependencies to keep Arrow Utils a lightweight dependency that can be effortlessly integrated into any project.

## Quick Start
To use Arrow Utils for data conversion, you first need to add it to your project. For projects using Maven, add the following dependency to your `pom.xml` file:
```xml
<dependency>
    <groupId>tech.dingxin</groupId>
    <artifactId>arrow-utils</artifactId>
    <version>0.0.1</version>
</dependency>
```

### From POJO to Arrow
```java
// Imports excluded
// Instantiate your POJO data list
POJO pojo = POJO.getSampleInstance();
// Create a PojoToArrowConverter
PojoToArrowConverter pojoToArrowConverter = new PojoToArrowConverter(POJO.class, null);
pojoToArrowConverter.newInstance();
// Convert POJO data to Arrow format
pojoToArrowConverter.write(pojo);
VectorSchemaRoot arrowBatch1 = pojoToArrowConverter.getArrowBatch();
// You can reset the converter using the reset() method
pojoToArrowConverter.reset();
pojoToArrowConverter.write(pojo);
pojoToArrowConverter.write(pojo);
VectorSchemaRoot arrowBatch2 = pojoToArrowConverter.getArrowBatch();
// Using Arrow format data...
```

### From JDBC Result to Arrow
```java
// Imports excluded
// Assume we already have a JDBC ResultSet
ResultSet resultSet = fetchResultSetFromDatabase();
// Convert JDBC ResultSet to Arrow format
List<ArrowRowData> rowData = new ArrayList<>();
ResultSetMetaData metaData = resultSet.getMetaData();
// Convert JDBC ResultSet metadata to Arrow Schema (using SQLite syntax)
Schema schema = JdbcUtils.toArrowSchema(metaData, SqlLiteDialect.INSTANCE);
while (resultSet.next()) {
    ArrowRowData row = new ArrowRowData(schema);
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
        row.set(i - 1, resultSet.getObject(i));
    }
    rowData.add(row);
}
// Initialize ArrowDataSerializer
ArrowDataSerializer serializer = new ArrowDataSerializer(schema, allocator);
serializer.addAll(rowData);
VectorSchemaRoot vectorSchemaRoot = serializer.getVectorSchemaRoot();
// Using Arrow format data...
```

## How to Contribute
Arrow Utils welcomes any form of contributions, whether it's new features, bug fixes, documentation updates, or optimization suggestions. Please follow the steps below to submit your contribution:
1. Fork this repository.
2. Create a new branch (e.g., `feature/my-new-feature` or `bugfix/my-fix`).
3. Commit your changes.
4. Create a Pull Request.

Please ensure your code is consistent with the existing code style and passes all tests.

## License
Arrow Utils is licensed under the [Apache License 2.0](LICENSE). For more information, refer to the LICENSE file.