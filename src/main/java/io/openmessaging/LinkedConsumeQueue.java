package io.openmessaging;

import io.openmessaging.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/4
 * Time: 下午1:43
 */
public class LinkedConsumeQueue {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /*文件块分配表*/
    //list里存的是这个topic第几块是总块数的第几块
    private final ConcurrentMap<String/* topic */, ArrayList<Integer>> blockAllocationTable;

    /*每个topic的长度*/
    private final ConcurrentMap<String/* topic */, AtomicInteger> queueCounter;

    /*文件的大小*/
    private final int mappedFileSize;

    /*文件块的大小*/
    private final int blockSize;

    private final int indexPerBlock;

    /*映射的文件名*/
    private static AtomicInteger fileName = new AtomicInteger(0);

    /*文件所在路径*/
    private final String storePath;

    /*映射的文件*/
    private File file;

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*每个索引单元的大小*/
    public static final int INDEX_UNIT_SIZE = 8;

    /*缓冲*/
//    private final ByteBuffer byteBufferIndex;

    /*总共分配了多少块*/
    private AtomicInteger lastBlock = new AtomicInteger(0);


    public LinkedConsumeQueue(final String storePath, int mappedFileSize, int blockSize) {
        this.blockAllocationTable = new ConcurrentHashMap<>();
        this.queueCounter = new ConcurrentHashMap<>();
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.blockSize = blockSize;
        this.indexPerBlock = this.blockSize / INDEX_UNIT_SIZE;
//        this.byteBufferIndex = ByteBuffer.allocate(INDEX_UNIT_SIZE);

        this.file = new File(this.storePath + File.separator + fileName.getAndIncrement());
        boolean ok = false;

        /*检查文件夹是否存在*/
        ensureDirOK(this.storePath);

        /*打开文件，并将文件映射到内存*/
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedFileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + fileName + " Failed. ", e);
        } catch (IOException e) {
            log.error("map file " + fileName + " Failed. ", e);
        } finally {
            if (!ok && this.fileChannel != null) {
                try {
                    this.fileChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private int getLastBlockNum(String topic, Integer counter) {

        if (!blockAllocationTable.containsKey(topic))
            blockAllocationTable.put(topic, new ArrayList<>());

        ArrayList<Integer> blockAllocationList = blockAllocationTable.get(topic);

        /*如果当前块已经写满，分配一个新的块*/
        if (counter % indexPerBlock == 0){
            blockAllocationList.add(lastBlock.getAndIncrement());
        }

        int lastBlockNum = blockAllocationList.get(blockAllocationList.size() - 1);
        return lastBlockNum;
    }

    /*同步添加消息索引*/
    boolean putMessagePositionInfo(final String topic, final long offset) {

        /*当前topic中消息的个数*/
        if (!queueCounter.containsKey(topic))
            queueCounter.put(topic, new AtomicInteger(0));

        AtomicInteger counter = queueCounter.get(topic);

        int indexOffset = counter.getAndIncrement();

        /*如果当前文件写满了*/
        int lastBlockNum = getLastBlockNum(topic, indexOffset);
        if (lastBlockNum * blockSize > mappedFileSize)
            return false;

        /*当前block中的偏移*/
        int blockOffset = (indexOffset % indexPerBlock) * INDEX_UNIT_SIZE;

        // 计算index的存储位置
        int expectLogicOffset = lastBlockNum * blockSize + blockOffset;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(expectLogicOffset);
        byteBuffer.putLong(offset);

        return true;
    }

    public long getMessagePosition(final String topic, final long indexOffset){
        if (!queueCounter.containsKey(topic))
            return -1;

        int maxIndexOffset = queueCounter.get(topic).get();

        if (indexOffset >= maxIndexOffset)
            return -1;

        int blockNum = (int) indexOffset / indexPerBlock;
        int blockOffset = (int) indexOffset % indexPerBlock;

        ArrayList<Integer> blockAllocationlist = blockAllocationTable.get(topic);
        int PhyBlockNum = blockAllocationlist.get(blockNum);

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(PhyBlockNum * blockSize + blockOffset * INDEX_UNIT_SIZE);


        return byteBuffer.getLong();
    }

    public int getSize(final String topic){
        if (!queueCounter.containsKey(topic))
            return -1;

        return queueCounter.get(topic).get();
    }


//    ArrayList<CommitLogEntryIndex> getMessagePositionList(final String topic, final long indexOffset, final int queryNum){
//        if (!queueCounter.containsKey(topic))
//            return null;
//
//        int maxIndexOffset = queueCounter.get(topic).get();
//
//        if (indexOffset >= maxIndexOffset)
//            return null;
//
//        int num = queryNum;
//        if (indexOffset + queryNum > maxIndexOffset)
//            num = (int)(maxIndexOffset - indexOffset);
//
//        /*第一个索引所在的逻辑块*/
//        int blockNum = (int) indexOffset / indexPerBlock;
//        int blockOffset = (int) indexOffset % indexPerBlock;
//
//        ArrayList<Integer> blockAllocationList = blockAllocationTable.get(topic);
//
//        int currentBlockNum = blockNum;
//        int currentBlockOffset = blockOffset;
//
//        /*当前逻辑块对应的物理块*/
//        int PhyBlockNum = blockAllocationList.get(currentBlockNum);
//
//        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
//        byteBuffer.position(PhyBlockNum * blockSize + currentBlockOffset * INDEX_UNIT_SIZE);
//        ByteBuffer currentBlockByteBuffer = byteBuffer.slice();
//        currentBlockByteBuffer.limit(blockSize);
//
//        ArrayList<CommitLogEntryIndex> messagePositionList = new ArrayList<>();
//
//        while (messagePositionList.size() < num){
//            if (currentBlockOffset == indexPerBlock){
//                currentBlockNum += 1;
//                if (currentBlockNum == blockAllocationList.size()) break;
//                PhyBlockNum = blockAllocationList.get(currentBlockNum);
////                currentBlockByteBuffer = this.mappedByteBuffer.slice();
////                currentBlockByteBuffer.position(PhyBlockNum * blockSize);
////                currentBlockByteBuffer.limit(PhyBlockNum * blockSize + blockSize);
//                byteBuffer = this.mappedByteBuffer.slice();
//                byteBuffer.position(PhyBlockNum * blockSize);
//                currentBlockByteBuffer = byteBuffer.slice();
//                currentBlockByteBuffer.limit(blockSize);
//
//                currentBlockOffset = 0;
//            }
//
//            long offsetPy = currentBlockByteBuffer.getLong();
//            int sizePy = currentBlockByteBuffer.getInt();
//            currentBlockOffset += 1;
//
//            messagePositionList.add(new CommitLogEntryIndex(offsetPy, sizePy));
//        }
//
//        return messagePositionList;
//    }


    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }


}
