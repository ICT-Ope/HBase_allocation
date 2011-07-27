/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memstore to disk.  To
 * create, call {@link #createWriter(FileSystem, Path, int)} and append data.  Be
 * sure to add any metadata before calling close on the Writer
 * (Use the appendMetadata convenience methods). On close, a StoreFile is
 * sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #createReader()}.
 * <p>StoreFiles may also reference store files in another Store.
 *
 * The reason for this weird pattern where you use a different instance for the
 * writer and a reader is that we write once but read a lot more.
 */
public class StoreFile {
  static final Log LOG = LogFactory.getLog(StoreFile.class.getName());

  // Config keys.
  static final String IO_STOREFILE_BLOOM_ERROR_RATE = "io.storefile.bloom.error.rate";
  static final String IO_STOREFILE_BLOOM_MAX_FOLD = "io.storefile.bloom.max.fold";
  static final String IO_STOREFILE_BLOOM_MAX_KEYS = "io.storefile.bloom.max.keys";
  static final String IO_STOREFILE_BLOOM_ENABLED = "io.storefile.bloom.enabled";
  static final String HFILE_BLOCK_CACHE_SIZE_KEY = "hfile.block.cache.size";

  public static enum BloomType {
    /**
     * Bloomfilters disabled
     */
    NONE,
    /**
     * Bloom enabled with Table row as Key
     */
    ROW,
    /**
     * Bloom enabled with Table row & column (family+qualifier) as Key
     */
    ROWCOL
  }
  // Keys for fileinfo values in HFile
  /** Max Sequence ID in FileInfo */
  public static final byte [] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");
  /** Major compaction flag in FileInfo */
  public static final byte [] MAJOR_COMPACTION_KEY = Bytes.toBytes("MAJOR_COMPACTION_KEY");
  /** Bloom filter Type in FileInfo */
  static final byte[] BLOOM_FILTER_TYPE_KEY = Bytes.toBytes("BLOOM_FILTER_TYPE");
  /** Key for Timerange information in metadata*/
  static final byte[] TIMERANGE_KEY = Bytes.toBytes("TIMERANGE");

  /** Meta data block name for bloom filter meta-info (ie: bloom params/specs) */
  static final String BLOOM_FILTER_META_KEY = "BLOOM_FILTER_META";
  /** Meta data block name for bloom filter data (ie: bloom bits) */
  static final String BLOOM_FILTER_DATA_KEY = "BLOOM_FILTER_DATA";

  // Make default block size for StoreFiles 8k while testing.  TODO: FIX!
  // Need to make it 8k for testing.
  public static final int DEFAULT_BLOCKSIZE_SMALL = 8 * 1024;


  private static BlockCache hfileBlockCache = null;

  private final FileSystem fs;
  // This file's path.
  private final Path path;
  // If this storefile references another, this is the reference instance.
  private Reference reference;
  // If this StoreFile references another, this is the other files path.
  private Path referencePath;
  // Should the block cache be used or not.
  private boolean blockcache;
  // Is this from an in-memory store
  private boolean inMemory;

  // Keys for metadata stored in backing HFile.
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  /** Meta key set when store file is a result of a bulk load */
  public static final byte[] BULKLOAD_TASK_KEY =
    Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  public static final byte[] BULKLOAD_TIME_KEY =
    Bytes.toBytes("BULKLOAD_TIMESTAMP");

  /**
   * Map of the metadata entries in the corresponding HFile
   */
  private Map<byte[], byte[]> metadataMap;

  /*
   * Regex that will work for straight filenames and for reference names.
   * If reference, then the regex has more than just one group.  Group 1 is
   * this files id.  Group 2 the referenced region name, etc.
   */
  private static final Pattern REF_NAME_PARSER =
    Pattern.compile("^(\\d+)(?:\\.(.+))?$");

  // StoreFile.Reader
  private volatile Reader reader;

  // Used making file ids.
  private final static Random rand = new Random();
  private final Configuration conf;
  private final BloomType bloomType;


  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a
   * substantial amount of ram depending on the underlying files (10-20MB?).
   *
   * @param fs  The current file system to use.
   * @param p  The path of the file.
   * @param blockcache  <code>true</code> if the block cache is enabled.
   * @param conf  The current configuration.
   * @param bt The bloom type to use for this store file
   * @throws IOException When opening the reader fails.
   */
  StoreFile(final FileSystem fs,
            final Path p,
            final boolean blockcache,
            final Configuration conf,
            final BloomType bt,
            final boolean inMemory)
      throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.path = p;
    this.blockcache = blockcache;
    this.inMemory = inMemory;
    if (isReference(p)) {
      this.reference = Reference.read(fs, p);
      this.referencePath = getReferredToFile(this.path);
    }
    // ignore if the column family config says "no bloom filter"
    // even if there is one in the hfile.
    if (conf.getBoolean(IO_STOREFILE_BLOOM_ENABLED, true)) {
      this.bloomType = bt;
    } else {
      this.bloomType = BloomType.NONE;
      LOG.info("Ignoring bloom filter check for file (disabled in config)");
    }
  }

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  Path getPath() {
    return this.path;
  }

  /**
   * @return The Store/ColumnFamily this file belongs to.
   */
  byte [] getFamily() {
    return Bytes.toBytes(this.path.getParent().getName());
  }

  /**
   * @return True if this is a StoreFile Reference; call after {@link #open()}
   * else may get wrong answer.
   */
  boolean isReference() {
    return this.reference != null;
  }

  /**
   * @param p Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p) {
    return !p.getName().startsWith("_") &&
      isReference(p, REF_NAME_PARSER.matcher(p.getName()));
  }

  /**
   * @param p Path to check.
   * @param m Matcher to use.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p, final Matcher m) {
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    return m.groupCount() > 1 && m.group(2) != null;
  }

  /*
   * Return path to the file referred to by a Reference.  Presumes a directory
   * hierarchy of <code>${hbase.rootdir}/tablename/regionname/familyname</code>.
   * @param p Path to a Reference file.
   * @return Calculated path to parent region file.
   * @throws IOException
   */
  static Path getReferredToFile(final Path p) {
    Matcher m = REF_NAME_PARSER.matcher(p.getName());
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    // Other region name is suffix on the passed Reference file name
    String otherRegion = m.group(2);
    // Tabledir is up two directories from where Reference was written.
    Path tableDir = p.getParent().getParent().getParent();
    String nameStrippedOfSuffix = m.group(1);
    // Build up new path with the referenced region in place of our current
    // region in the reference path.  Also strip regionname suffix from name.
    return new Path(new Path(new Path(tableDir, otherRegion),
      p.getParent().getName()), nameStrippedOfSuffix);
  }

  /**
   * @return True if this file was made by a major compaction.
   */
  boolean isMajorCompaction() {
    if (this.majorCompaction == null) {
      throw new NullPointerException("This has not been set yet");
    }
    return this.majorCompaction.get();
  }

  /**
   * @return This files maximum edit sequence id.
   */
  public long getMaxSequenceId() {
    return this.sequenceid;
  }

  /**
   * Return the highest sequence ID found across all storefiles in
   * the given list. Store files that were created by a mapreduce
   * bulk load are ignored, as they do not correspond to any edit
   * log items.
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxSequenceIdInList(List<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      if (!sf.isBulkLoadResult()) {
        max = Math.max(max, sf.getMaxSequenceId());
      }
    }
    return max;
  }

  /**
   * @return true if this storefile was created by HFileOutputFormat
   * for a bulk load.
   */
  boolean isBulkLoadResult() {
    return metadataMap.containsKey(BULKLOAD_TIME_KEY);
  }

  /**
   * Return the timestamp at which this bulk load file was generated.
   */
  public long getBulkLoadTimestamp() {
    return Bytes.toLong(metadataMap.get(BULKLOAD_TIME_KEY));
  }

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  public static synchronized BlockCache getBlockCache(Configuration conf) {
    if (hfileBlockCache != null) return hfileBlockCache;

    float cachePercentage = conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.2f);
    // There should be a better way to optimize this. But oh well.
    if (cachePercentage == 0L) return null;
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HFILE_BLOCK_CACHE_SIZE_KEY +
        " must be between 0.0 and 1.0, not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long)(mu.getMax() * cachePercentage);
    LOG.info("Allocating LruBlockCache with maximum size " +
      StringUtils.humanReadableInt(cacheSize));
    hfileBlockCache = new LruBlockCache(cacheSize, DEFAULT_BLOCKSIZE_SMALL);
    return hfileBlockCache;
  }

  /**
   * @return the blockcache
   */
  public BlockCache getBlockCache() {
    return blockcache ? getBlockCache(conf) : null;
  }

  /**
   * Opens reader on this store file.  Called by Constructor.
   * @return Reader for the store file.
   * @throws IOException
   * @see #closeReader()
   */
  private Reader open() throws IOException {
    if (this.reader != null) {
      throw new IllegalAccessError("Already open");
    }
    if (isReference()) {
      this.reader = new HalfStoreFileReader(this.fs, this.referencePath,
          getBlockCache(), this.reference);
    } else {
      this.reader = new Reader(this.fs, this.path, getBlockCache(),
          this.inMemory);
    }
    // Load up indices and fileinfo.
    metadataMap = Collections.unmodifiableMap(this.reader.loadFileInfo());
    // Read in our metadata.
    byte [] b = metadataMap.get(MAX_SEQ_ID_KEY);
    if (b != null) {
      // By convention, if halfhfile, top half has a sequence number > bottom
      // half. Thats why we add one in below. Its done for case the two halves
      // are ever merged back together --rare.  Without it, on open of store,
      // since store files are distingushed by sequence id, the one half would
      // subsume the other.
      this.sequenceid = Bytes.toLong(b);
      if (isReference()) {
        if (Reference.isTopFileRegion(this.reference.getFileRegion())) {
          this.sequenceid += 1;
        }
      }
    }
    this.reader.setSequenceID(this.sequenceid);

    b = metadataMap.get(MAJOR_COMPACTION_KEY);
    if (b != null) {
      boolean mc = Bytes.toBoolean(b);
      if (this.majorCompaction == null) {
        this.majorCompaction = new AtomicBoolean(mc);
      } else {
        this.majorCompaction.set(mc);
      }
    } else {
      // Presume it is not major compacted if it doesn't explicity say so
      // HFileOutputFormat explicitly sets the major compacted key.
      this.majorCompaction = new AtomicBoolean(false);
    }

    if (this.bloomType != BloomType.NONE) {
      this.reader.loadBloomfilter();
    }

    try {
      byte [] timerangeBytes = metadataMap.get(TIMERANGE_KEY);
      if (timerangeBytes != null) {
        this.reader.timeRangeTracker = new TimeRangeTracker();
        Writables.copyWritable(timerangeBytes, this.reader.timeRangeTracker);
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error reading timestamp range data from meta -- " +
          "proceeding without", e);
      this.reader.timeRangeTracker = null;
    }
    return this.reader;
  }

  /**
   * @return Reader for StoreFile. creates if necessary
   * @throws IOException
   */
  public Reader createReader() throws IOException {
    if (this.reader == null) {
      this.reader = open();
    }
    return this.reader;
  }

  /**
   * @return Current reader.  Must call createReader first else returns null.
   * @throws IOException
   * @see #createReader()
   */
  public Reader getReader() {
    return this.reader;
  }

  /**
   * @throws IOException
   */
  public synchronized void closeReader() throws IOException {
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }

  /**
   * Delete this file
   * @throws IOException
   */
  public void deleteReader() throws IOException {
    closeReader();
    this.fs.delete(getPath(), true);
  }

  @Override
  public String toString() {
    return this.path.toString() +
      (isReference()? "-" + this.referencePath + "-" + reference.toString(): "");
  }

  /**
   * @return a length description of this StoreFile, suitable for debug output
   */
  public String toStringDetailed() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.path.toString());
    sb.append(", isReference=").append(isReference());
    sb.append(", isBulkLoadResult=").append(isBulkLoadResult());
    if (isBulkLoadResult()) {
      sb.append(", bulkLoadTS=").append(getBulkLoadTimestamp());
    } else {
      sb.append(", seqid=").append(getMaxSequenceId());
    }
    sb.append(", majorCompaction=").append(isMajorCompaction());

    return sb.toString();
  }

  /**
   * Utility to help with rename.
   * @param fs
   * @param src
   * @param tgt
   * @return True if succeeded.
   * @throws IOException
   */
  public static Path rename(final FileSystem fs,
                            final Path src,
                            final Path tgt)
      throws IOException {

    if (!fs.exists(src)) {
      throw new FileNotFoundException(src.toString());
    }
    if (!fs.rename(src, tgt)) {
      throw new IOException("Failed rename of " + src + " to " + tgt);
    }
    return tgt;
  }

  /**
   * Get a store file writer. Client is responsible for closing file when done.
   *
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize size per filesystem block
   * @return StoreFile.Writer
   * @throws IOException
   */
  public static Writer createWriter(final FileSystem fs,
                                              final Path dir,
                                              final int blocksize)
      throws IOException {

    return createWriter(fs, dir, blocksize, null, null, null, BloomType.NONE, 0);
  }

  /**
   * Create a store file writer. Client is responsible for closing file when done.
   * If metadata, add BEFORE closing using appendMetadata()
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize
   * @param algorithm Pass null to get default.
   * @param conf HBase system configuration. used with bloom filters
   * @param bloomType column family setting for bloom filters
   * @param c Pass null to get default.
   * @param maxKeySize peak theoretical entry size (maintains error rate)
   * @return HFile.Writer
   * @throws IOException
   */
  public static StoreFile.Writer createWriter(final FileSystem fs,
                                              final Path dir,
                                              final int blocksize,
                                              final Compression.Algorithm algorithm,
                                              final KeyValue.KVComparator c,
                                              final Configuration conf,
                                              BloomType bloomType,
                                              int maxKeySize)
      throws IOException {

    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    Path path = getUniqueFile(fs, dir);
    if(conf == null || !conf.getBoolean(IO_STOREFILE_BLOOM_ENABLED, true)) {
      bloomType = BloomType.NONE;
    }

    return new Writer(fs, path, blocksize,
        algorithm == null? HFile.DEFAULT_COMPRESSION_ALGORITHM: algorithm,
        conf, c == null? KeyValue.COMPARATOR: c, bloomType, maxKeySize);
  }

  /**
   * @param fs
   * @param dir Directory to create file in.
   * @return random filename inside passed <code>dir</code>
   */
  public static Path getUniqueFile(final FileSystem fs, final Path dir)
      throws IOException {
    if (!fs.getFileStatus(dir).isDir()) {
      throw new IOException("Expecting " + dir.toString() +
        " to be a directory");
    }
    return fs.getFileStatus(dir).isDir()? getRandomFilename(fs, dir): dir;
  }

  /**
   *
   * @param fs
   * @param dir
   * @return Path to a file that doesn't exist at time of this invocation.
   * @throws IOException
   */
  static Path getRandomFilename(final FileSystem fs, final Path dir)
      throws IOException {
    return getRandomFilename(fs, dir, null);
  }

  /**
   *
   * @param fs
   * @param dir
   * @param suffix
   * @return Path to a file that doesn't exist at time of this invocation.
   * @throws IOException
   */
  static Path getRandomFilename(final FileSystem fs,
                                final Path dir,
                                final String suffix)
      throws IOException {
    long id = -1;
    Path p = null;
    do {
      id = Math.abs(rand.nextLong());
      p = new Path(dir, Long.toString(id) +
        ((suffix == null || suffix.length() <= 0)? "": suffix));
    } while(fs.exists(p));
    return p;
  }

  /**
   * Write out a split reference.
   *
   * Package local so it doesnt leak out of regionserver.
   *
   * @param fs
   * @param splitDir Presumes path format is actually
   * <code>SOME_DIRECTORY/REGIONNAME/FAMILY</code>.
   * @param f File to split.
   * @param splitRow
   * @param range
   * @return Path to created reference.
   * @throws IOException
   */
  static Path split(final FileSystem fs,
                    final Path splitDir,
                    final StoreFile f,
                    final byte [] splitRow,
                    final Reference.Range range)
      throws IOException {
    // A reference to the bottom half of the hsf store file.
    Reference r = new Reference(splitRow, range);
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_PARSER regex above.  The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = f.getPath().getParent().getParent().getName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    return r.write(fs, p);
  }


  /**
   * A StoreFile writer.  Use this to read/write HBase Store Files. It is package
   * local because it is an implementation detail of the HBase regionserver.
   */
  public static class Writer {
    private final BloomFilter bloomFilter;
    private final BloomType bloomType;
    private KVComparator kvComparator;
    private KeyValue lastKv = null;
    private byte[] lastByteArray = null;
    TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
    /* isTimeRangeTrackerSet keeps track if the timeRange has already been set
     * When flushing a memstore, we set TimeRange and use this variable to
     * indicate that it doesn't need to be calculated again while
     * appending KeyValues.
     * It is not set in cases of compactions when it is recalculated using only
     * the appended KeyValues*/
    boolean isTimeRangeTrackerSet = false;

    protected HFile.Writer writer;
    /**
     * Creates an HFile.Writer that also write helpful meta data.
     * @param fs file system to write to
     * @param path file name to create
     * @param blocksize HDFS block size
     * @param compress HDFS block compression
     * @param conf user configuration
     * @param comparator key comparator
     * @param bloomType bloom filter setting
     * @param maxKeys maximum amount of keys to add (for blooms)
     * @throws IOException problem writing to FS
     */
    public Writer(FileSystem fs, Path path, int blocksize,
        Compression.Algorithm compress, final Configuration conf,
        final KVComparator comparator, BloomType bloomType, int maxKeys)
        throws IOException {
      writer = new HFile.Writer(fs, path, blocksize, compress, comparator.getRawComparator());

      this.kvComparator = comparator;

      BloomFilter bloom = null;
      BloomType bt = BloomType.NONE;

      if (bloomType != BloomType.NONE && conf != null) {
        float err = conf.getFloat(IO_STOREFILE_BLOOM_ERROR_RATE, (float)0.01);
        // Since in row+col blooms we have 2 calls to shouldSeek() instead of 1
        // and the false positives are adding up, we should keep the error rate
        // twice as low in order to maintain the number of false positives as
        // desired by the user
        if (bloomType == BloomType.ROWCOL) {
          err /= 2;
        }
        int maxFold = conf.getInt(IO_STOREFILE_BLOOM_MAX_FOLD, 7);
        int tooBig = conf.getInt(IO_STOREFILE_BLOOM_MAX_KEYS, 128*1000*1000);
        
        if (maxKeys < tooBig) { 
          try {
            bloom = new ByteBloomFilter(maxKeys, err,
                Hash.getHashType(conf), maxFold);
            bloom.allocBloom();
            bt = bloomType;
          } catch (IllegalArgumentException iae) {
            LOG.warn(String.format(
              "Parse error while creating bloom for %s (%d, %f)", 
              path, maxKeys, err), iae);
            bloom = null;
            bt = BloomType.NONE;
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping bloom filter because max keysize too large: " 
                + maxKeys);
          }
        }
      }

      this.bloomFilter = bloom;
      this.bloomType = bt;
    }

    /**
     * Writes meta data.
     * Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @throws IOException problem writing to FS
     */
    public void appendMetadata(final long maxSequenceId, final boolean majorCompaction)
    throws IOException {
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      writer.appendFileInfo(MAJOR_COMPACTION_KEY,
          Bytes.toBytes(majorCompaction));
      appendTimeRangeMetadata();
    }

    /**
     * Add TimestampRange to Metadata
     */
    public void appendTimeRangeMetadata() throws IOException {
      appendFileInfo(TIMERANGE_KEY,WritableUtils.toByteArray(timeRangeTracker));
    }

    /**
     * Set TimeRangeTracker
     * @param trt
     */
    public void setTimeRangeTracker(final TimeRangeTracker trt) {
      this.timeRangeTracker = trt;
      isTimeRangeTrackerSet = true;
    }

    /**
     * If the timeRangeTracker is not set,
     * update TimeRangeTracker to include the timestamp of this key
     * @param kv
     * @throws IOException
     */
    public void includeInTimeRangeTracker(final KeyValue kv) {
      if (!isTimeRangeTrackerSet) {
        timeRangeTracker.includeTimestamp(kv);
      }
    }

    /**
     * If the timeRangeTracker is not set,
     * update TimeRangeTracker to include the timestamp of this key
     * @param key
     * @throws IOException
     */
    public void includeInTimeRangeTracker(final byte [] key) {
      if (!isTimeRangeTrackerSet) {
        timeRangeTracker.includeTimestamp(key);
      }
    }

    public void append(final KeyValue kv) throws IOException {
      if (this.bloomFilter != null) {
        // only add to the bloom filter on a new, unique key
        boolean newKey = true;
        if (this.lastKv != null) {
          switch(bloomType) {
          case ROW:
            newKey = ! kvComparator.matchingRows(kv, lastKv);
            break;
          case ROWCOL:
            newKey = ! kvComparator.matchingRowColumn(kv, lastKv);
            break;
          case NONE:
            newKey = false;
          }
        }
        if (newKey) {
          /*
           * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.png
           * Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + TimeStamp
           *
           * 2 Types of Filtering:
           *  1. Row = Row
           *  2. RowCol = Row + Qualifier
           */
          switch (bloomType) {
          case ROW:
            this.bloomFilter.add(kv.getBuffer(), kv.getRowOffset(),
                kv.getRowLength());
            break;
          case ROWCOL:
            // merge(row, qualifier)
            int ro = kv.getRowOffset();
            int rl = kv.getRowLength();
            int qo = kv.getQualifierOffset();
            int ql = kv.getQualifierLength();
            byte [] result = new byte[rl + ql];
            System.arraycopy(kv.getBuffer(), ro, result, 0,  rl);
            System.arraycopy(kv.getBuffer(), qo, result, rl, ql);
            this.bloomFilter.add(result);
            break;
          default:
          }
          this.lastKv = kv;
        }
      }
      writer.append(kv);
      includeInTimeRangeTracker(kv);
    }

    public Path getPath() {
      return this.writer.getPath();
    }
    
    boolean hasBloom() { 
      return this.bloomFilter != null;
    }

    public void append(final byte [] key, final byte [] value) throws IOException {
      if (this.bloomFilter != null) {
        // only add to the bloom filter on a new row
        if (this.lastByteArray == null || !Arrays.equals(key, lastByteArray)) {
          this.bloomFilter.add(key);
          this.lastByteArray = key;
        }
      }
      writer.append(key, value);
      includeInTimeRangeTracker(key);
    }

    public void close() throws IOException {
      // make sure we wrote something to the bloom before adding it
      if (this.bloomFilter != null && this.bloomFilter.getKeyCount() > 0) {
        bloomFilter.compactBloom();
        if (this.bloomFilter.getMaxKeys() > 0) {
          int b = this.bloomFilter.getByteSize();
          int k = this.bloomFilter.getKeyCount();
          int m = this.bloomFilter.getMaxKeys();
          StoreFile.LOG.info("Bloom added to HFile (" + 
              getPath() + "): " + StringUtils.humanReadableInt(b) + ", " +
              k + "/" + m + " (" + NumberFormat.getPercentInstance().format(
                ((double)k) / ((double)m)) + ")");
        }
        writer.appendMetaBlock(BLOOM_FILTER_META_KEY, bloomFilter.getMetaWriter());
        writer.appendMetaBlock(BLOOM_FILTER_DATA_KEY, bloomFilter.getDataWriter());
        writer.appendFileInfo(BLOOM_FILTER_TYPE_KEY, Bytes.toBytes(bloomType.toString()));
      }
      writer.close();
    }

    public void appendFileInfo(byte[] key, byte[] value) throws IOException {
      writer.appendFileInfo(key, value);
    }
  }

  /**
   * Reader for a StoreFile.
   */
  public static class Reader {
    static final Log LOG = LogFactory.getLog(Reader.class.getName());

    protected BloomFilter bloomFilter = null;
    protected BloomType bloomFilterType;
    private final HFile.Reader reader;
    protected TimeRangeTracker timeRangeTracker = null;
    protected long sequenceID = -1;

    public Reader(FileSystem fs, Path path, BlockCache blockCache, boolean inMemory)
        throws IOException {
      reader = new HFile.Reader(fs, path, blockCache, inMemory);
      bloomFilterType = BloomType.NONE;
    }

    public RawComparator<byte []> getComparator() {
      return reader.getComparator();
    }

    /**
     * Get a scanner to scan over this StoreFile.
     *
     * @param cacheBlocks should this scanner cache blocks?
     * @param pread use pread (for highly concurrent small readers)
     * @return a scanner
     */
    public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread) {
      return new StoreFileScanner(this, getScanner(cacheBlocks, pread));
    }

    /**
     * Warning: Do not write further code which depends on this call. Instead
     * use getStoreFileScanner() which uses the StoreFileScanner class/interface
     * which is the preferred way to scan a store with higher level concepts.
     *
     * @param cacheBlocks should we cache the blocks?
     * @param pread use pread (for concurrent small readers)
     * @return the underlying HFileScanner
     */
    @Deprecated
    public HFileScanner getScanner(boolean cacheBlocks, boolean pread) {
      return reader.getScanner(cacheBlocks, pread);
    }

    public void close() throws IOException {
      reader.close();
    }

    public boolean shouldSeek(Scan scan, final SortedSet<byte[]> columns) {
        return (passesTimerangeFilter(scan) && passesBloomFilter(scan,columns));
    }

    /**
     * Check if this storeFile may contain keys within the TimeRange
     * @param scan
     * @return False if it definitely does not exist in this StoreFile
     */
    private boolean passesTimerangeFilter(Scan scan) {
      if (timeRangeTracker == null) {
        return true;
      } else {
        return timeRangeTracker.includesTimeRange(scan.getTimeRange());
      }
    }

    private boolean passesBloomFilter(Scan scan, final SortedSet<byte[]> columns) {
      if (this.bloomFilter == null || !scan.isGetScan()) {
        return true;
      }
      byte[] row = scan.getStartRow();
      byte[] key;
      switch (this.bloomFilterType) {
        case ROW:
          key = row;
          break;
        case ROWCOL:
          if (columns != null && columns.size() == 1) {
            byte[] col = columns.first();
            key = Bytes.add(row, col);
            break;
          }
          //$FALL-THROUGH$
        default:
          return true;
      }

      try {
        ByteBuffer bloom = reader.getMetaBlock(BLOOM_FILTER_DATA_KEY, true);
        if (bloom != null) {
          if (this.bloomFilterType == BloomType.ROWCOL) {
            // Since a Row Delete is essentially a DeleteFamily applied to all
            // columns, a file might be skipped if using row+col Bloom filter.
            // In order to ensure this file is included an additional check is
            // required looking only for a row bloom.
            return this.bloomFilter.contains(key, bloom) ||
                this.bloomFilter.contains(row, bloom);
          }
          else {
            return this.bloomFilter.contains(key, bloom);
          }
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter data -- proceeding without",
            e);
        setBloomFilterFaulty();
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter data -- proceeding without", e);
        setBloomFilterFaulty();
      }

      return true;
    }

    public Map<byte[], byte[]> loadFileInfo() throws IOException {
      Map<byte [], byte []> fi = reader.loadFileInfo();

      byte[] b = fi.get(BLOOM_FILTER_TYPE_KEY);
      if (b != null) {
        bloomFilterType = BloomType.valueOf(Bytes.toString(b));
      }

      return fi;
    }

    public void loadBloomfilter() {
      if (this.bloomFilter != null) {
        return; // already loaded
      }

      try {
        ByteBuffer b = reader.getMetaBlock(BLOOM_FILTER_META_KEY, false);
        if (b != null) {
          if (bloomFilterType == BloomType.NONE) {
            throw new IOException("valid bloom filter type not found in FileInfo");
          }


          this.bloomFilter = new ByteBloomFilter(b);
          LOG.info("Loaded " + (bloomFilterType== BloomType.ROW? "row":"col")
                 + " bloom filter metadata for " + reader.getName());
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter meta -- proceeding without", e);
        this.bloomFilter = null;
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter meta -- proceeding without", e);
        this.bloomFilter = null;
      }
    }

    public int getFilterEntries() {
      return (this.bloomFilter != null) ? this.bloomFilter.getKeyCount()
          : reader.getFilterEntries();
    }

    public ByteBuffer getMetaBlock(String bloomFilterDataKey, boolean cacheBlock) throws IOException {
      return reader.getMetaBlock(bloomFilterDataKey, cacheBlock);
    }

    public void setBloomFilterFaulty() {
      bloomFilter = null;
    }

    public byte[] getLastKey() {
      return reader.getLastKey();
    }

    public byte[] midkey() throws IOException {
      return reader.midkey();
    }

    public long length() {
      return reader.length();
    }

    public int getEntries() {
      return reader.getEntries();
    }

    public byte[] getFirstKey() {
      return reader.getFirstKey();
    }

    public long indexSize() {
      return reader.indexSize();
    }

    public BloomType getBloomFilterType() {
      return this.bloomFilterType;
    }

    public long getSequenceID() {
      return sequenceID;
    }

    public void setSequenceID(long sequenceID) {
      this.sequenceID = sequenceID;
    }
  }

  /**
   * Useful comparators for comparing StoreFiles.
   */
  abstract static class Comparators {
    /**
     * Comparator that compares based on the flush time of
     * the StoreFiles. All bulk loads are placed before all non-
     * bulk loads, and then all files are sorted by sequence ID.
     * If there are ties, the path name is used as a tie-breaker.
     */
    static final Comparator<StoreFile> FLUSH_TIME =
      Ordering.compound(ImmutableList.of(
          Ordering.natural().onResultOf(new GetBulkTime()),
          Ordering.natural().onResultOf(new GetSeqId()),
          Ordering.natural().onResultOf(new GetPathName())
      ));

    private static class GetBulkTime implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (!sf.isBulkLoadResult()) return Long.MAX_VALUE;
        return sf.getBulkLoadTimestamp();
      }
    }
    private static class GetSeqId implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (sf.isBulkLoadResult()) return -1L;
        return sf.getMaxSequenceId();
      }
    }
    private static class GetPathName implements Function<StoreFile, String> {
      @Override
      public String apply(StoreFile sf) {
        return sf.getPath().getName();
      }
    }

  }
}
