package com.datatorrent.dimensions.mysql.app;

import com.datatorrent.api.DefaultInputPort;
import java.util.Collection;

import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;

public class CustomDimensionsStoreHDHT extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final long serialVersionUID = 201510020305L;

  @InputPortFieldAnnotation(optional=true)
  public final transient DefaultInputPort<Aggregate> otherPort = new DefaultInputPort<Aggregate>() {

    @Override
    public void process(Aggregate t)
    {
      CustomDimensionsStoreHDHT.this.processEvent(t);
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    this.futureBuckets.clear();

    super.beginWindow(windowId);
  }

  @Override
  public Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> definePartitions(Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> partitions, PartitioningContext context)
  {
    Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> newPartitions = super.definePartitions(partitions, context);

    long bucket = ((AppDataSingleSchemaDimensionStoreHDHT) newPartitions.iterator().next().getPartitionedInstance()).getBucketID();

    // assign the partition keys
    if (context.getParallelPartitionCount() == 0) {
      DefaultPartition.assignPartitionKeys(newPartitions, input);
    }

    for (Partition<AbstractSinglePortHDHTWriter<Aggregate>> p : newPartitions) {
      CustomDimensionsStoreHDHT tempStore = ((CustomDimensionsStoreHDHT) p.getPartitionedInstance());
      tempStore.setBucketID(bucket);

      if (context.getParallelPartitionCount() != 0) {
        tempStore.partitionMask = 0;
        tempStore.partitions = Sets.newHashSet(0);
      }

      bucket++;
    }

    return newPartitions;
  }
}
