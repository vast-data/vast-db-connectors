package ndb.view;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Seq;

import java.util.Arrays;
import java.util.function.BiFunction;

import static spark.sql.catalog.ndb.VastCatalog.DEFAULT_VAST_CATALOG;

public class VastNamespaceResolver
        implements BiFunction<Seq<String>, String[], String[]>
{
    private static final Logger LOG = LoggerFactory.getLogger(VastNamespaceResolver.class);

    @Override
    public String[] apply(Seq<String> fullName, String[] currentNamespace)
    {
        int uRelNameLength = fullName.size();
        String[] namespaceForLookup;
        LOG.debug("Trying to resolve namespace. fullName: {}, currentNamespace: {}", fullName, Arrays.toString(currentNamespace));
        if (uRelNameLength == 1) {
            return currentNamespace;
        }
        else {
            final int startIndex = indexOfStripDefaultCatalogFromFullName(fullName);
            final int namespaceForLookupLength = uRelNameLength - 1 - startIndex;
            namespaceForLookup = new String[namespaceForLookupLength];
            for (int i = 0; i < namespaceForLookup.length; i++) { // skip "ndb"
                namespaceForLookup[i] = fullName.apply(startIndex + i);
            }
            LOG.debug("Resolved namespace: {}", Arrays.toString(namespaceForLookup));
            return namespaceForLookup;
        }
    }

    private int indexOfStripDefaultCatalogFromFullName(Seq<String> uRelName)
    {
        return uRelName.apply(0).equals(DEFAULT_VAST_CATALOG[0]) ? 1 : 0;
    }
}
