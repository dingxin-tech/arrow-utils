# Arrow Utils

[English Version README](README_en.md)

Arrow Utils 是一个开源工具库，它提供便捷的方法将各种数据类型转换成 Apache Arrow 格式。这个项目旨在简化使用 Java 实现数据处理、传输和存储过程中涉及到的数据格式转换。无论数据来源是普通的 POJO、通过 JDBC 获取的数据集，还是其他数据结构，Arrow Utils 都能够帮助你轻松转换成高性能的 Apache Arrow 数据格式。

## 特性

- **POJO 到 Arrow 格式的转换**：轻松将你的 Java Plain Old Java Objects (POJOs) 转换为 Arrow 数据结构。
- **JDBC 结果集到 Arrow 格式的转换**：允许你直接从 JDBC 结果集（目前支持SqlLite语法）将数据转换为 Arrow 格式，有效利用 Arrow 的内存效率和速度。
- **支持多种数据源**：除了 POJO 和 JDBC 结果集，未来还可能添加对其他数据源的支持，如 CSV、JSON、Parquet 等。
- **轻量级**：尽量减少依赖，让 Arrow Utils 成为一个轻量级的依赖，在任何项目中都能轻松集成。

## 快速开始

要使用 Arrow Utils 进行数据转换，首先需要将其添加到你的项目中。使用 Maven 的项目可以添加以下依赖到 `pom.xml` 文件：

```xml
<dependency>
    <groupId>tech.dingxin</groupId>
    <artifactId>arrow-utils</artifactId>
    <version>0.0.1</version>
</dependency>
```

### 从 POJO 转换到 Arrow

```java
// 省略 imports

// 实例化你的 POJO 数据列表
POJO pojo = POJO.getSampleInstance();

// 创建一个 PojoToArrowConverter
PojoToArrowConverter pojoToArrowConverter = new PojoToArrowConverter(POJO.class, null);
pojoToArrowConverter.newInstance();

// 转换 POJO 数据到 Arrow 格式
pojoToArrowConverter.write(pojo);
VectorSchemaRoot arrowBatch1 = pojoToArrowConverter.getArrowBatch();

// 可以通过 reset() 方法重置转换器
pojoToArrowConverter.reset();
pojoToArrowConverter.write(pojo);
pojoToArrowConverter.write(pojo);
VectorSchemaRoot arrowBatch2 = pojoToArrowConverter.getArrowBatch();

// 使用 Arrow 格式的数据...
```

### 从 JDBC 结果转换到 Arrow

```java
// 省略 imports

// 假设我们已经有了一个 JDBC ResultSet
ResultSet resultSet = fetchResultSetFromDatabase();

// 转换 JDBC ResultSet 到 Arrow 格式
List<ArrowRowData> rowData = new ArrayList<>();
ResultSetMetaData metaData = resultSet.getMetaData();

// 转换 JDBC ResultSet 元数据到 Arrow Schema（使用SqlLite语法）
Schema schema = JdbcUtils.toArrowSchema(metaData, SqlLiteDialect.INSTANCE);

while (resultSet.next()) {
    ArrowRowData row = new ArrowRowData(schema);
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
        row.set(i - 1, resultSet.getObject(i));
    }
    rowData.add(row);
}

// 初始化 ArrowDataSerializer
ArrowDataSerializer serializer = new ArrowDataSerializer(schema, allocator);
serializer.addAll(rowData);
VectorSchemaRoot vectorSchemaRoot = serializer.getVectorSchemaRoot();

// 使用 Arrow 格式的数据...
```

## 如何贡献

Arrow Utils 项目欢迎任何形式的贡献，无论是新功能、bug 修复、文档更新还是优化建议。请按照以下步骤提交您的贡献：

1. Fork 本仓库。
2. 创建一个新的分支（比如 `feature/my-new-feature` 或 `bugfix/my-fix`）。
3. 提交你的更改。
4. 发起一个 Pull Request。

请确保你的代码与现有代码风格保持一致，并且通过所有测试。

## 许可证

Arrow Utils 使用 [Apache License 2.0](LICENSE)。详情请参阅 LICENSE 文件。