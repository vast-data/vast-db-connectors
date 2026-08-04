/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.google.common.collect.ImmutableList;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.spark.VastColumnarBatchReader;
import com.vastdata.spark.VastInputPartition;
import com.vastdata.spark.VastView;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.vastdata.client.schema.VastViewMetadata.COLUMN_ALIASES_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.COLUMN_COMMENTS_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.COMMENT_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.SQL_FIELD;
import static com.vastdata.client.schema.VastViewMetadata.VIEW_METADATA_TABLE;
import static java.lang.String.format;

public class SparkViewsMetadataReader
{

    public static final StructType VIEW_DETAILS_SCHEMA = TypeUtil.arrowFieldsListToSparkSchema(
            ImmutableList.of(SQL_FIELD, COLUMN_ALIASES_FIELD,
                    COLUMN_COMMENTS_FIELD, COMMENT_FIELD));
    public static final VastInputPartition PARTITION = new VastInputPartition(0,
            0, 1, 1);
    public static final QueryDataExtraParams EXTRA_QUERY_PARAMS = new QueryDataExtraParams();

    static {
        EXTRA_QUERY_PARAMS.addExtraQueryParams(
                QueryDataExtraParams.QueryDataExtraParamType.URL_PARAM,
            "sub-table", VIEW_METADATA_TABLE);}
    private final VastConfig config;

    SparkViewsMetadataReader(VastConfig config)
    {
        this.config = config;
    }

    private static String[] rawObjectsArrayToStringsArray(
            Object[] rawAliasArray)
    {
        return rawAliasArray == null ? new String[0] : Arrays
                .stream(rawAliasArray)
                .map(o -> o == null ? null : o.toString())
                .toArray(String[]::new);
    }

    public VastView getVastView(SimpleVastTransaction tx, String schemaName,
            String viewName, String[] viewNamespace, List<Field> viewDataFields,
            VastSchedulingInfo schedulingInfo, String endUser)
    {
        StructType viewDataSchema = new StructType(viewDataFields
                .stream()
                .map(TypeUtil::arrowFieldToSparkField)
                .toArray(StructField[]::new));
        try (VastColumnarBatchReader batchReader = new VastColumnarBatchReader(
                tx, 0, config, schemaName, viewName, PARTITION,
                VIEW_DETAILS_SCHEMA, 1, Collections.emptyList(), schedulingInfo,
                false, EXTRA_QUERY_PARAMS, endUser)) {
            while (batchReader.next()) {
                ColumnarBatch columnarBatch = batchReader.get();
                if (columnarBatch.numRows() > 0) {
                    InternalRow row = columnarBatch.getRow(0);
                    String sqlString = row.getUTF8String(0).toString();
                    String[] aliases = rawObjectsArrayToStringsArray(
                            row.getArray(1).array());
                    String[] colComments = rawObjectsArrayToStringsArray(
                            row.getArray(2).array());
                    String comment = row.getString(3);
                    return new VastView(viewName, sqlString, "ndb", comment,
                            viewNamespace, viewDataSchema, aliases, aliases,
                            colComments);
                }
            }
            throw new RuntimeException(
                    format("Failed to load view metadata: %s,%s", schemaName,
                            viewName));
        }
    }
}
