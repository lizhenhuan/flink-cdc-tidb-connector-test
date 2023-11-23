import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.TableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.gson.Gson;

import java.util.HashMap;
import java.util.stream.Collectors;

public class TiDBSourceExample {

    public static void main(String[] args) throws Exception {
        String databaseName = "test";
        String tableName = "test1";
        TiConfiguration tiConf = TDBSourceOptions.getTiConfiguration(
                "10.2.103.64:2389,10.2.103.28:2389", new HashMap<>());
        TiSession session = TiSession.create(tiConf);
        TiTableInfo tableInfo = session.getCatalog().getTable(databaseName, tableName);
        tableInfo.getColumns().stream().filter((TiColumnInfo columnInfo) -> columnInfo.isPrimaryKey()).forEach((TiColumnInfo columnInfo) -> System.out.println(columnInfo.getName()));
        SourceFunction<String> tidbSource =
                TiDBSource.<String>builder()
                        .database(databaseName) // set captured database
                        .tableName(tableName) // set captured table
                        .tiConf(tiConf)
                        .snapshotEventDeserializer(

                                new TiKVSnapshotEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Kvrpcpb.KvPair record, Collector<String> out)
                                            throws Exception {
                                        Object[] tikvValues = TableCodec.decodeObjects(record.getValue().toByteArray(), RowKey.decode(record.getKey().toByteArray()).getHandle(), tableInfo);
                                        Gson gson = new Gson();

                                        out.collect("###" + RowKey.decode(record.getKey().toByteArray()) + gson.toJson(tikvValues));
                                        out.collect("###" + record.toString());

                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })

                        .changeEventDeserializer(
                                new TiKVChangeEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Cdcpb.Event.Row record, Collector<String> out)
                                            throws Exception {
                                        Object[] tikvValues = TableCodec.decodeObjects(record.getValue().toByteArray(), RowKey.decode(record.getKey().toByteArray()).getHandle(), tableInfo);
                                        Gson gson = new Gson();
                                        //out.collect("@@@@" + RowKey.decode(record.getKey().toByteArray()) + gson.toJson(tikvValues));
                                        out.collect("@@@" + record.toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        //env.enableCheckpointing(3000);
        env.addSource(tidbSource).print().setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");
    }
}