package org.apache.bookkeeper.bookie.storage.ldb.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

public class InvalidByteBufAllocator implements ByteBufAllocator {

    @Override
    public ByteBuf buffer() {
        return null;
    }

    @Override
    public ByteBuf buffer(int i) {
        return null;
    }

    @Override
    public ByteBuf buffer(int i, int i1) {
        return null;
    }

    @Override
    public ByteBuf ioBuffer() {
        return null;
    }

    @Override
    public ByteBuf ioBuffer(int i) {
        return null;
    }

    @Override
    public ByteBuf ioBuffer(int i, int i1) {
        return null;
    }

    @Override
    public ByteBuf heapBuffer() {
        return null;
    }

    @Override
    public ByteBuf heapBuffer(int i) {
        return null;
    }

    @Override
    public ByteBuf heapBuffer(int i, int i1) {
        return null;
    }

    @Override
    public ByteBuf directBuffer() {
        return null;
    }

    @Override
    public ByteBuf directBuffer(int i) {
        return null;
    }

    @Override
    public ByteBuf directBuffer(int i, int i1) {
        return null;
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return null;
    }

    @Override
    public CompositeByteBuf compositeBuffer(int i) {
        return null;
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return null;
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int i) {
        return null;
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return null;
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int i) {
        return null;
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }

    @Override
    public int calculateNewCapacity(int i, int i1) {
        return 0;
    }
}
