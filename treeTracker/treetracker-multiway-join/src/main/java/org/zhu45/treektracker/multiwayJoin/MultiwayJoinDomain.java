package org.zhu45.treektracker.multiwayJoin;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import org.zhu45.treetracker.common.BaseDomain;
import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcRecordSetProvider;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.RecordSetProvider;
import org.zhu45.treetracker.jdbc.RecordTupleSource;
import org.zhu45.treetracker.jdbc.RecordTupleSourceProvider;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class MultiwayJoinDomain
        extends BaseDomain<MultiwayJoinDomain, Row>
        implements AutoCloseable
{
    private static final LoggerProvider.TreeTrackerLogger log = LoggerProvider.getLogger(MultiwayJoinDomain.class);

    private JdbcClient jdbcClient;
    private RecordTupleSource<? extends Row> source;
    private boolean useDomainAsSource;

    /**
     * WARNING: this constructor will create ResultSet object in heap, which can be very large
     * if the underlying table specified by schemaTableName is large. This has performance indication
     * and use wisely.
     */
    public MultiwayJoinDomain(SchemaTableName schemaTableName, JdbcClient jdbcClient, Class<? extends RecordTupleSourceProvider<? extends Row>> recordTupleSourceProviderClazz)
    {
        super();
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        JdbcTableHandle jdbcTableHandle = jdbcClient.getTableHandle(schemaTableName);
        List<JdbcColumnHandle> jdbcColumnHandleList = jdbcClient.getColumns(jdbcTableHandle);
        RecordTupleSourceProvider<? extends Row> recordTupleSourceProvider = null;
        try {
            Constructor<? extends RecordTupleSourceProvider<? extends Row>> constructor = recordTupleSourceProviderClazz.getConstructor(RecordSetProvider.class);
            recordTupleSourceProvider = constructor.newInstance(new JdbcRecordSetProvider(this.jdbcClient));
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        source = recordTupleSourceProvider.createTupleSource(jdbcTableHandle, jdbcColumnHandleList);
    }

    public MultiwayJoinDomain()
    {
        super();
    }

    @Override
    public Iterator<Row> iterator()
    {
        if (Switches.DEBUG && log.isDebugEnabled()) {
            log.debug("use MultiwayJoinDomainIterator iterator");
        }
        if (isEmpty() && !useDomainAsSource) {
            return new MultiwayJoinDomainIterator(source);
        }
        return super.iterator();
    }

    @Override
    public boolean remove(Row v)
    {
        // Whenever v is removed, the underlying tuple in data source
        // shouldn't be read again. If v does not present in the memory domain set,
        // calling super.remove() is correct. If v presents, since we directly delete
        // the memory set copy, tuple in data source will not be deleted. Since once we have
        // iterate through each tuple in the data source, those tuples will be inside the memory set
        // and whenever iterator reset, the iterator will be set to the memory domain set iterator,
        // and thus, the deleted tuple will not be read again from data source. In other words, we don't
        // need to add any special marker to the tuples in the data source to mark them not to be read ("deleted"
        // from TT-2 perspective).
        return super.remove(v);
    }

    @Override
    public void close()
    {
        if (source != null) {
            source.close();
        }
    }

    @Override
    public Domain<MultiwayJoinDomain, Row> deepCopy()
    {
        return new MultiwayJoinDomain();
    }

    static class MultiwayJoinDomainIterator
            implements Iterator<Row>
    {
        private final RecordTupleSource source;

        MultiwayJoinDomainIterator(RecordTupleSource source)
        {
            this.source = requireNonNull(source, "ObjectRecordTupleSource is null");
            this.source.reset();
        }

        @Override
        public boolean hasNext()
        {
            return source.hasNext();
        }

        @Override
        public Row next()
        {
            return source.getNextRow();
        }
    }

    protected RecordTupleSource<? extends Row> getSource()
    {
        return source;
    }

    @Override
    public String toString()
    {
        List<Row> domainVals = super.getDomainAsList();
        return Joiner.on(',').join(domainVals);
    }

    public RowSet getDomainAsRowSet()
    {
        return new RowSet(getDomainAsList());
    }

    public void setRowSetAsDomain(RowSet rowSet)
    {
        super.setDomainVals(rowSet.getRows());
    }

    public JdbcClient getJdbcClient()
    {
        return this.jdbcClient;
    }

    public void setUseDomainAsSource(boolean useDomainAsSource)
    {
        this.useDomainAsSource = useDomainAsSource;
    }

    public boolean getUseDomainAsSource()
    {
        return this.useDomainAsSource;
    }
}
