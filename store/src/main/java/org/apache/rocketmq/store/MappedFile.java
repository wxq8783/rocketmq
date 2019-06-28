/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    //操作系统每页大写  默认4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //当前JVM实例中MappedFile虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    //当前JVM实例中MappedFile对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    //当前该文件的写指针 从0开始(内存映射文件中的写指针)
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //ADD BY ChenYang  当前文件的提交指针，如果开启transientStorePoolEnable，则数据会存储在TransientStorePool中  然后提交到内存映射ByteBuffer中，再刷写到磁盘
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    //刷写到磁盘指针  该指针之前的数据持久化到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    //文件大写
    protected int fileSize;
    //文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆内存ByteBuffer  如果不为空，数据首先将存储在该Buffer中，然后提交到MappedFile对应的内存映射文件Buffer。transientStorePoolEnable为true时不为空
     */
    protected ByteBuffer writeBuffer = null;
    //堆内存池  transientStorePoolEnable=true时启用
    protected TransientStorePool transientStorePool = null;
    //文件名称
    private String fileName;
    //该文件的初始偏移量
    private long fileFromOffset;
    //物理文件
    private File file;
    //物理文件对应的内存映射Buffer
    private MappedByteBuffer mappedByteBuffer;
    //文件最后一次内容写入时间
    private volatile long storeTimestamp = 0;
    //是否是MappedFileQueue队列中第一个文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     *  两种创建MappedFile之  使用Mmap的方式来构建
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 两种创建MappedFile之 从TransientStorePool堆外内池中获取相应的DirectByteBuffer来构建
     */
    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    //堆外内存的清除操作
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 根据transientStorePoolEnable是否开启存在两种初始化情况  transientStorePoolEnable=true表示优先线程在堆外内存
     * 然后通过commit线程提交到内存映射Buffer中，在通过Flush线程将内存映射Buffer中的数据持久化到磁盘
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */

    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        //堆外内存池
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        //当前的这个MappedFile的写入位置
        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            //这个Buffer和同步/异步刷盘相关(异步刷盘还有两种模式可供选择)
            //通过slice()创建一个与MappedFile的共享内存区  并设置position为当前指针
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = null;
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }
    //将内容追加到内存映射文件中
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘
     * 将内存中的数据刷写到自评 永久存储在磁盘中
     * 有两种方式
     * 1、fileChannel.force(false);
     * 2、mappedByteBuffer.force();  就是mmap 零拷贝
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {

                //int value = this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
                //如果 writeBuffer 为空， 则直接返回当前的写指针；如果 writeBuffer 不为空， 则返回上一次提交的指针 。
                int value = getReadPosition();

                //如果writeBuffer不为空，则flushedPosition应等于上一次commit指针，因为上一次提交的数据就是进入到MappedByteBuffer中的数据
                //writeBuffer为空，数据是直接进入到MappedByteBuffer，writePosition代表的是MappedByteBuffer中的指针  设置flushedPosition为writePosition
                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 内存映射文件的提交动作
     * @param commitLeastPages  本次提交最小的页数
     * @return
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 具体的提交实现 作用就是 将MappedFile的writeBuffer中的数据提交到文件通道FileChannel中
     *
     * ByteBuffer 使用技巧：slice()方法创建一个共享缓存区, 与原先的ByteBuffer共享内存
     * 但维护一套独立的指针(position 、 mark 、 limit)
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                //创建writeBuffer的共享缓存区
                ByteBuffer byteBuffer = writeBuffer.slice();
                //将新创建的position回退到上次提交的位置(committedPosition)
                byteBuffer.position(lastCommittedPosition);
                //设置limit为writePosition(当前最大有效数据指针)
                byteBuffer.limit(writePos);

                this.fileChannel.position(lastCommittedPosition);
                //然后把committedPosition到writePosition的数据复制(写入)到FileChannel这种
                this.fileChannel.write(byteBuffer);
                //更新committedPosition指针为writePosition
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否可提交
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();
        //如果文件已满 则可以提交
        if (this.isFull()) {
            return true;
        }
        //比较wrotePosition(当writeBuffer的写指针)与上一次提交的指针(committedPosition)的差值 除以 OS_PAGE_SIZE 得到当前脏页的数量   如果大于commitLeastPages 则提交
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        //如果commitLeastPages小于0  表示 只要存在脏页就提交
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 获取消息的内容
     * 采用mappedByteBuffer 方式
     * @param pos
     * @param size
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 查找pos到当前最大可读之间的数据
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                //由于在整个写入期间都未曾改变 MappedByteBuffer 的指针 ，所以mappedByteBuffer.slice()方法返回的共享缓存区空间为整个MappedFile
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                //
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }
        //如果是堆外内存 调用堆外内存的cleanup方法清除
        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * MappedFile销毁
     *
     * 在整个 MappedFile 销毁过程，首先需要释放资源，释放资源的前提条件是该 MappedFile 的引用小于等于 0
     * @param intervalForcibly
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        //关闭MappedFile
        this.shutdown(intervalForcibly);
        //判断是否清理完成
        if (this.isCleanupOver()) {
            try {
                //关闭文件通道，删除物理文件
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 获取MappedFile最大有效数据偏移量
     *
     * RocketMQ文件的一个组织方式是内存映射文件，预先申请一块连续的固定大小的内存，需要一套指针标识当前最大有效数据的位置
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        //
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 文件预热
     *
     * Page Cache 叫做页缓存，而每一页的大小通常是4K，
     * 在Linux系统中写入数据的时候并不会直接写到硬盘上，而是会先写到Page Cache中，并打上dirty标识，
     * 由内核线程flusher定期将被打上dirty的页发送给IO调度层，最后由IO调度决定何时落地到磁盘中，
     * 而Linux一般会把还没有使用的内存全拿来给Page Cache使用。
     * 而读的过程也是类似，会先到Page Cache中寻找是否有数据，有的话直接返回，
     * 如果没有才会到磁盘中去读取并写入Page Cache然后再次读取Page Cache并返回。
     * 而且读的这个过程中操作系统也会有一个预读的操作，你的每一次读取操作系统都会帮你预读出后面一部分数据，
     * 而且当你一直在使用预读数据的时候，系统会帮你预读出更多的数据(最大到128K)。
     *
     *
     * mmap是一种将文件映射到虚拟内存的技术，
     * 可以将文件在磁盘位置的地址和在虚拟内存中的虚拟地址通过映射对应起来，
     * 之后就可以在内存这块区域进行读写数据，而不必调用系统级别的read,wirte这些函数，从而提升IO操作性能，
     * 另外一点就是mmap后的虚拟内存大小必须是内存页大小(通常是4K)的倍数，之所以这么做是为了匹配内存操作。
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    //MMAP方式 实现零拷贝
    //将当前映射文件全部的地址空间锁定在物理存储中，防止其被交换到swap空间
    //在调用madvise，传入WILL_NEED策略，将刚刚锁住的内存预热， 起始就是告诉内核，我马上要用 这块内存，先做虚拟内存到物理内存的映射
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
