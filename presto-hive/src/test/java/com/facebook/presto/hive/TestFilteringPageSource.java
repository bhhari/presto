/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.orc.OrcSelectivePageSource;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PageSinkProperties;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.slice.Slices;
import io.airlift.tpch.Nation;
import io.airlift.tpch.NationColumn;
import io.airlift.tpch.NationGenerator;
import io.airlift.tpch.TpchColumnType;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveTestUtils.PAGE_SORTER;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveBatchPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveSelectivePageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultOrcFileWriterFactory;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.LocationHandle.TableType.NEW;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.TestHiveUtil.createTestingFileHiveMetastore;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertTrue;

public class TestFilteringPageSource
{
    private static final int NUM_ROWS = 1000;
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";
    private File tempDir;

    @BeforeClass
    public void init()
    {
        tempDir = Files.createTempDir();
        writeNationTableInAllFormats();
    }

    @AfterClass
    public void afterClass()
            throws IOException
    {
        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCompareOrcAndRcOutputWithPushdownEnabled()
    {
        //filter
        HiveColumnHandle regionKey = new HiveColumnHandle("n_regionkey", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), 2, REGULAR, Optional.empty());
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(regionKey, NullableValue.of(BIGINT, 0L)));

        //pushdown property enabled
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        hiveClientConfig.setPushdownFilterEnabled(true);

        //create page source from orc file
        ConnectorPageSource orcSource = createPageSource(new HiveTransactionHandle(), hiveClientConfig, new MetastoreClientConfig(), tupleDomain, getOutputFile(new File(tempDir, "ORC.NONE").getPath()));
        assertTrue(orcSource instanceof OrcSelectivePageSource);

        //set HiveStorageFormat to RC and read the RC file with pushdownEnabled in hiveLayout
        hiveClientConfig.setHiveStorageFormat(HiveStorageFormat.RCBINARY);
        ConnectorPageSource rcSource = createPageSource(new HiveTransactionHandle(), hiveClientConfig, new MetastoreClientConfig(), tupleDomain, getOutputFile(new File(tempDir, "RCBINARY.NONE").getPath()));
        assertTrue(rcSource instanceof FilteringPageSource);

        List<Page> orcPages = extractPages(orcSource);
        List<Page> rcPages = extractPages(rcSource);
        MaterializedResult expectedResults = toMaterializedResult(getSession(hiveClientConfig), getColumnTypes(), orcPages);
        MaterializedResult results = toMaterializedResult(getSession(hiveClientConfig), getColumnTypes(), rcPages);
        assertEquals(results, expectedResults);
    }

    private List<Page> extractPages(ConnectorPageSource orcSource)
    {
        List<Page> pages = new ArrayList<>();
        while (!orcSource.isFinished()) {
            Page nextPage = orcSource.getNextPage();
            if (nextPage != null) {
                pages.add(nextPage.getLoadedPage());
            }
        }
        return pages;
    }

    public void writeNationTableInAllFormats()
    {
        HiveClientConfig config = new HiveClientConfig();
        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
        for (HiveStorageFormat format : ImmutableList.of(HiveStorageFormat.ORC, HiveStorageFormat.RCBINARY, HiveStorageFormat.TEXTFILE)) {
            config.setHiveStorageFormat(format);
            config.setCompressionCodec(NONE);
            for (HiveCompressionCodec codec : HiveCompressionCodec.values()) {
                config.setCompressionCodec(codec);
                writeTestFile(config, new MetastoreClientConfig(), metastore, makeFileName(tempDir, config));
            }
        }
    }

    private static String makeFileName(File tempDir, HiveClientConfig config)
    {
        return tempDir.getAbsolutePath() + "/" + config.getHiveStorageFormat().name() + "." + config.getCompressionCodec().name();
    }

    private static ConnectorPageSink createPageSink(HiveTransactionHandle transaction, HiveClientConfig config, MetastoreClientConfig metastoreClientConfig, ExtendedHiveMetastore metastore, Path outputPath, HiveWriterStats stats)
    {
        LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, Optional.empty(), NEW, DIRECT_TO_TARGET_NEW_DIRECTORY);
        HiveOutputTableHandle handle = new HiveOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                getColumnHandles(),
                "test",
                new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                locationHandle,
                config.getHiveStorageFormat(),
                config.getHiveStorageFormat(),
                config.getCompressionCodec(),
                ImmutableList.of(),
                Optional.empty(),
                "test",
                ImmutableMap.of());

        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        HdfsEnvironment hdfsEnvironment = createTestHdfsEnvironment(config, metastoreClientConfig);

        HivePageSinkProvider provider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(config, metastoreClientConfig),
                hdfsEnvironment,
                PAGE_SORTER,
                metastore,
                new GroupByHashPageIndexerFactory(new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig())),
                TYPE_MANAGER,
                config,
                metastoreClientConfig,
                new HiveLocationService(hdfsEnvironment),
                partitionUpdateCodec,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                new HiveSessionProperties(config, new OrcFileWriterConfig(), new ParquetFileWriterConfig()),
                stats,
                getDefaultOrcFileWriterFactory(config, metastoreClientConfig));
        return provider.createPageSink(transaction, getSession(config), handle, PageSinkProperties.defaultProperties());
    }

    private static void writeTestFile(HiveClientConfig config, MetastoreClientConfig metastoreClientConfig, ExtendedHiveMetastore metastore, String outputPath)
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle();
        HiveWriterStats stats = new HiveWriterStats();
        ConnectorPageSink pageSink = createPageSink(transaction, config, metastoreClientConfig, metastore, new Path("file:///" + outputPath), stats);
        List<NationColumn> columns = Stream.of(NationColumn.values()).collect(toList());
        List<Type> columnTypes = getColumnTypes();

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        int rows = 0;
        for (Nation lineItem : new NationGenerator()) {
            rows++;
            if (rows >= NUM_ROWS) {
                break;
            }
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                NationColumn column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                switch (column.getType().getBase()) {
                    case IDENTIFIER:
                        BIGINT.writeLong(blockBuilder, column.getIdentifier(lineItem));
                        break;
                    case INTEGER:
                        INTEGER.writeLong(blockBuilder, column.getInteger(lineItem));
                        break;
                    case DATE:
                        DATE.writeLong(blockBuilder, column.getDate(lineItem));
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(lineItem));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(lineItem)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
        }
        Page page = pageBuilder.build();
        pageSink.appendPage(page);
        getFutureValue(pageSink.finish());
    }

    private static File getOutputFile(String outputPath)
    {
        File outputDir = new File(outputPath);
        List<File> files = ImmutableList.copyOf(outputDir.listFiles((dir, name) -> !name.endsWith(".crc")));
        return getOnlyElement(files);
    }

    @NotNull
    private static List<Type> getColumnTypes()
    {
        return Stream.of(NationColumn.values())
                .map(NationColumn::getType)
                .map(TestFilteringPageSource::getHiveType)
                .map(hiveType -> hiveType.getType(TYPE_MANAGER))
                .collect(toList());
    }

    private static MaterializedResult toMaterializedResult(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            if (outputPage.getPositionCount() > 0) {
                resultBuilder.page(outputPage);
            }
        }
        return resultBuilder.build();
    }

    private ConnectorPageSource createPageSource(HiveTransactionHandle transaction, HiveClientConfig config,
            MetastoreClientConfig metastoreClientConfig, TupleDomain<HiveColumnHandle> tupleDomain, File outputFile)
    {
        HiveSplit split = new HiveSplit(SCHEMA_NAME, TABLE_NAME, "", "file:///" + outputFile.getAbsolutePath(), 0,
                outputFile.length(), outputFile.length(),
                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(),
                                config.getHiveStorageFormat().getInputFormat(),
                                config.getHiveStorageFormat().getOutputFormat()),
                        "location", Optional.empty(), false, ImmutableMap.of()),
                ImmutableList.of(), ImmutableList.of(), OptionalInt.empty(), OptionalInt.empty(), false, 1,
                ImmutableMap.of(), Optional.empty(), false, Optional.empty());

        TupleDomain<Subfield> domainPredicate = tupleDomain.transform(HiveColumnHandle.class::cast)
                .transform(column -> new Subfield(column.getName(), ImmutableList.of()));

        TableHandle tableHandle = new TableHandle(
                new ConnectorId(HIVE_CATALOG),
                new HiveTableHandle(SCHEMA_NAME, TABLE_NAME),
                transaction,
                Optional.of(
                        new HiveTableLayoutHandle(
                                new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                                ImmutableList.of(),
                                getColumnHandles().stream()
                                        .map(column -> new Column(column.getName(), column.getHiveType(), Optional.empty()))
                                        .collect(toImmutableList()),
                                ImmutableMap.of(),
                                domainPredicate,
                                TRUE_CONSTANT,
                                tupleDomain.getDomains().get().keySet().stream().collect(toMap(p -> p.getName(), p -> p)),
                                TupleDomain.all(),
                                Optional.empty(),
                                Optional.empty(),
                                true,
                                "layout")));

        HivePageSourceProvider provider = new HivePageSourceProvider(
                config,
                createTestHdfsEnvironment(config, metastoreClientConfig),
                getDefaultHiveRecordCursorProvider(config, metastoreClientConfig),
                getDefaultHiveBatchPageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveSelectivePageSourceFactories(config, metastoreClientConfig),
                TYPE_MANAGER,
                ROW_EXPRESSION_SERVICE);

        return provider.createPageSource(transaction, getSession(config), split, tableHandle.getLayout().get(), ImmutableList.copyOf(getColumnHandles()));
    }

    private static TestingConnectorSession getSession(HiveClientConfig config)
    {
        return new TestingConnectorSession(
                new HiveSessionProperties(config, new OrcFileWriterConfig(), new ParquetFileWriterConfig())
                        .getSessionProperties());
    }

    private static List<HiveColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<NationColumn> columns = Stream.of(NationColumn.values()).collect(toList());
        for (int i = 0; i < columns.size(); i++) {
            NationColumn column = columns.get(i);
            HiveType hiveType = getHiveType(column.getType());
            handles.add(new HiveColumnHandle(column.getColumnName(), hiveType, hiveType.getTypeSignature(), i, REGULAR,
                    Optional.empty()));
        }
        return handles.build();
    }

    private static HiveType getHiveType(TpchColumnType type)
    {
        switch (type.getBase()) {
            case IDENTIFIER:
                return HIVE_LONG;
            case INTEGER:
                return HIVE_INT;
            case DATE:
                return HIVE_DATE;
            case DOUBLE:
                return HIVE_DOUBLE;
            case VARCHAR:
                return HIVE_STRING;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
