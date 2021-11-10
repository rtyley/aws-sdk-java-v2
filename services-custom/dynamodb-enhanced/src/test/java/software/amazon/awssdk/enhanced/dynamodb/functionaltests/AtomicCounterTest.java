package software.amazon.awssdk.enhanced.dynamodb.functionaltests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.functionaltests.models.RecordForPartialUpdates;

public class AtomicCounterTest extends LocalDynamoDbSyncTestBase {
    private static final String STRING_VALUE = "string value";
    private static final String RECORD_ID = "id123";

    private static final TableSchema<RecordForPartialUpdates> TABLE_SCHEMA =
            TableSchema.fromClass(RecordForPartialUpdates.class);

    private final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                                                                                .dynamoDbClient(getDynamoDbClient())
                                                                                .build();

    private final DynamoDbTable<RecordForPartialUpdates> mappedTable =
            enhancedClient.table(getConcreteTableName("table-name"), TABLE_SCHEMA);

    @Before
    public void createTable() {
        mappedTable.createTable(r -> r.provisionedThroughput(getDefaultProvisionedThroughput()));
    }

    @After
    public void deleteTable() {
        getDynamoDbClient().deleteTable(r -> r.tableName(getConcreteTableName("table-name")));
    }

    @Test
    public void defaultCounterIncrementsCorrectly() {
        RecordForPartialUpdates record = new RecordForPartialUpdates();
        record.setId(RECORD_ID);
        record.setAttribute1(STRING_VALUE);
        mappedTable.updateItem(record);

        RecordForPartialUpdates persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getAttribute1()).isEqualTo(STRING_VALUE);
        assertThat(persistedRecord.getDefaultCounter()).isEqualTo(1L);

        mappedTable.updateItem(record);
        persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getAttribute1()).isEqualTo(STRING_VALUE);
        assertThat(persistedRecord.getDefaultCounter()).isEqualTo(2L);
    }

    @Test
    public void settingCounterValueHasNoEffect() {
        RecordForPartialUpdates record = new RecordForPartialUpdates();
        record.setId(RECORD_ID);
        record.setDefaultCounter(10L);
        record.setAttribute1(STRING_VALUE);
        mappedTable.updateItem(record);

        RecordForPartialUpdates persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getAttribute1()).isEqualTo(STRING_VALUE);
        assertThat(persistedRecord.getDefaultCounter()).isEqualTo(1L);
    }

    @Test
    public void counterWithCustomValues() {
        RecordForPartialUpdates record = new RecordForPartialUpdates();
        record.setId(RECORD_ID);
        record.setAttribute1(STRING_VALUE);
        mappedTable.updateItem(record);

        RecordForPartialUpdates persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getAttribute1()).isEqualTo(STRING_VALUE);
        assertThat(persistedRecord.getCustomCounter()).isEqualTo(15L);

        mappedTable.updateItem(record);
        persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getCustomCounter()).isEqualTo(20L);
    }

    @Test
    public void counterCanDecreaseValue() {
        RecordForPartialUpdates record = new RecordForPartialUpdates();
        record.setId(RECORD_ID);
        record.setAttribute1(STRING_VALUE);
        mappedTable.updateItem(record);

        RecordForPartialUpdates persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getAttribute1()).isEqualTo(STRING_VALUE);
        assertThat(persistedRecord.getDecreasingCounter()).isEqualTo(-21L);
    }

    @Test
    public void counterInitializedWithPut() {
        RecordForPartialUpdates record = new RecordForPartialUpdates();
        record.setId(RECORD_ID);
        record.setAttribute1(STRING_VALUE);
        mappedTable.putItem(record);

        RecordForPartialUpdates persistedRecord = mappedTable.getItem(record);
        assertThat(persistedRecord.getAttribute1()).isEqualTo(STRING_VALUE);
        assertThat(persistedRecord.getDefaultCounter()).isEqualTo(1L);
    }
}
