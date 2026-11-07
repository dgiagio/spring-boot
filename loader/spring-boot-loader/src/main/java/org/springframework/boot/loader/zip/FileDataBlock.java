/*
 * Copyright 2012-present the original author or authors.
 */

package org.springframework.boot.loader.zip;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.springframework.boot.loader.log.DebugLogger;

/**
 * Reference counted {@link DataBlock} implementation backed by a {@link File} with
 * support for slicing.
 *
 * @author Phillip Webb
 */
class FileDataBlock implements CloseableDataBlock {

	private static final DebugLogger debug = DebugLogger.get(FileDataBlock.class);

	static Tracker tracker = Tracker.NONE;

	private final FileAccess fileAccess;

	private final long offset;

	private final long size;

	FileDataBlock(Path path) throws IOException {
		this.fileAccess = new FileAccess(path);
		this.offset = 0;
		this.size = Files.size(path);
	}

	FileDataBlock(FileAccess fileAccess, long offset, long size) {
		this.fileAccess = fileAccess;
		this.offset = offset;
		this.size = size;
	}

	@Override
	public long size() throws IOException {
		return this.size;
	}

	@Override
	public int read(ByteBuffer dst, long pos) throws IOException {
		if (pos < 0) {
			throw new IllegalArgumentException("Position must not be negative");
		}
		ensureOpen(ClosedChannelException::new);
		long remaining = this.size - pos;
		if (remaining <= 0) {
			return -1;
		}
		int originalDestinationLimit = -1;
		if (dst.remaining() > remaining) {
			originalDestinationLimit = dst.limit();
			long updatedLimit = dst.position() + remaining;
			dst.limit((updatedLimit > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) updatedLimit);
		}
		int result = this.fileAccess.read(dst, this.offset + pos);
		if (originalDestinationLimit != -1) {
			dst.limit(originalDestinationLimit);
		}
		return result;
	}

	/**
	 * Open a connection to this block, increasing the reference count and re-opening the
	 * underlying file channel if necessary.
	 * @throws IOException on I/O error
	 */
	void open() throws IOException {
		this.fileAccess.open();
	}

	/**
	 * Close a connection to this block, decreasing the reference count and closing the
	 * underlying file channel if necessary.
	 * @throws IOException on I/O error
	 */
	@Override
	public void close() throws IOException {
		this.fileAccess.close();
	}

	/**
	 * Ensure that the underlying file channel is currently open.
	 * @param exceptionSupplier a supplier providing the exception to throw
	 * @param <E> the exception type
	 * @throws E if the channel is closed
	 */
	<E extends Exception> void ensureOpen(Supplier<E> exceptionSupplier) throws E {
		this.fileAccess.ensureOpen(exceptionSupplier);
	}

	/**
	 * Return a new {@link FileDataBlock} slice providing access to a subset of the data.
	 * The caller is responsible for calling {@link #open()} and {@link #close()} on the
	 * returned block.
	 * @param offset the start offset for the slice relative to this block
	 * @return a new {@link FileDataBlock} instance
	 * @throws IOException on I/O error
	 */
	FileDataBlock slice(long offset) throws IOException {
		return slice(offset, this.size - offset);
	}

	/**
	 * Return a new {@link FileDataBlock} slice providing access to a subset of the data.
	 * The caller is responsible for calling {@link #open()} and {@link #close()} on the
	 * returned block.
	 * @param offset the start offset for the slice relative to this block
	 * @param size the size of the new slice
	 * @return a new {@link FileDataBlock} instance
	 */
	FileDataBlock slice(long offset, long size) {
		if (offset == 0 && size == this.size) {
			return this;
		}
		if (offset < 0) {
			throw new IllegalArgumentException("Offset must not be negative");
		}
		if (size < 0 || offset + size > this.size) {
			throw new IllegalArgumentException("Size must not be negative and must be within bounds");
		}
		debug.log("Slicing %s at %s with size %s", this.fileAccess, offset, size);
		return new FileDataBlock(this.fileAccess, this.offset + offset, size);
	}

	/**
	 * Manages access to underlying {@link FileChannel}.
	 * <p>
	 * Reads are served from a read-ahead buffer that is guarded by a lock. A thread that
	 * finds the lock held reads directly from the channel instead of waiting for it. An
	 * interrupt during a channel read closes the channel for every thread, so a direct
	 * read that finds the channel closed falls back to a read under the lock. That read
	 * reopens the channel, or fails if the last reference has been closed. The reference
	 * count is only updated under the lock when the channel needs to be opened or closed.
	 */
	static class FileAccess {

		static final int BUFFER_SIZE = 1024 * 10;

		private static final AtomicIntegerFieldUpdater<FileAccess> referenceCountUpdater = AtomicIntegerFieldUpdater
			.newUpdater(FileAccess.class, "referenceCount");

		private final Path path;

		private volatile int referenceCount;

		private volatile FileChannel fileChannel;

		private RandomAccessFile randomAccessFile;

		private ByteBuffer buffer;

		private long bufferPosition = -1;

		private int bufferSize;

		private final ReentrantLock lock = new ReentrantLock();

		FileAccess(Path path) {
			if (!Files.isRegularFile(path)) {
				throw new IllegalArgumentException(path + " must be a regular file");
			}
			this.path = path;
		}

		int read(ByteBuffer dst, long position) throws IOException {
			if (!this.lock.tryLock()) {
				FileChannel fileChannel = this.fileChannel;
				if (fileChannel != null && !Thread.currentThread().isInterrupted()) {
					int dstPosition = dst.position();
					try {
						return readDirectly(fileChannel, dst, position);
					}
					catch (ClosedChannelException ex) {
						dst.position(dstPosition);
					}
				}
				this.lock.lock();
			}
			try {
				return readUsingBuffer(dst, position);
			}
			finally {
				this.lock.unlock();
			}
		}

		private int readDirectly(FileChannel fileChannel, ByteBuffer dst, long position) throws IOException {
			int limit = dst.limit();
			dst.limit(dst.position() + Math.min(dst.remaining(), BUFFER_SIZE));
			try {
				return fileChannel.read(dst, position);
			}
			finally {
				dst.limit(limit);
			}
		}

		private int readUsingBuffer(ByteBuffer dst, long position) throws IOException {
			if (this.fileChannel == null) {
				throw new ClosedChannelException();
			}
			if (position < this.bufferPosition || position >= this.bufferPosition + this.bufferSize) {
				fillBuffer(position);
			}
			if (this.bufferSize <= 0) {
				return this.bufferSize;
			}
			int offset = (int) (position - this.bufferPosition);
			int length = Math.min(this.bufferSize - offset, dst.remaining());
			dst.put(dst.position(), this.buffer, offset, length);
			dst.position(dst.position() + length);
			return length;
		}

		private void fillBuffer(long position) throws IOException {
			this.bufferPosition = -1;
			this.bufferSize = 0;
			if (Thread.currentThread().isInterrupted()) {
				fillBufferUsingRandomAccessFile(position);
				return;
			}
			try {
				FileChannel fileChannel = this.fileChannel;
				if (!fileChannel.isOpen()) {
					fileChannel = repairFileChannel();
				}
				this.buffer.clear();
				this.bufferSize = fileChannel.read(this.buffer, position);
				this.bufferPosition = position;
			}
			catch (ClosedChannelException ex) {
				// This thread or a thread reading directly was interrupted
				fillBufferUsingRandomAccessFile(position);
			}
		}

		private void fillBufferUsingRandomAccessFile(long position) throws IOException {
			if (this.randomAccessFile == null) {
				this.randomAccessFile = new RandomAccessFile(this.path.toFile(), "r");
				tracker.openedFileChannel(this.path);
			}
			byte[] bytes = new byte[BUFFER_SIZE];
			this.randomAccessFile.seek(position);
			int len = this.randomAccessFile.read(bytes);
			this.buffer.clear();
			if (len > 0) {
				this.buffer.put(bytes, 0, len);
			}
			this.bufferSize = len;
			this.bufferPosition = position;
		}

		private FileChannel repairFileChannel() throws IOException {
			tracker.closedFileChannel(this.path);
			FileChannel fileChannel = FileChannel.open(this.path, StandardOpenOption.READ);
			this.fileChannel = fileChannel;
			tracker.openedFileChannel(this.path);
			return fileChannel;
		}

		void open() throws IOException {
			int referenceCount = updateReferenceCountWithoutLock(1);
			if (referenceCount == 0) {
				this.lock.lock();
				try {
					if (this.referenceCount == 0) {
						debug.log("Opening '%s'", this.path);
						this.fileChannel = FileChannel.open(this.path, StandardOpenOption.READ);
						this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
						tracker.openedFileChannel(this.path);
					}
					referenceCount = referenceCountUpdater.incrementAndGet(this);
				}
				finally {
					this.lock.unlock();
				}
			}
			debug.log("Reference count for '%s' incremented to %s", this.path, referenceCount);
		}

		void close() throws IOException {
			int referenceCount = updateReferenceCountWithoutLock(-1);
			if (referenceCount == 0) {
				this.lock.lock();
				try {
					if (this.referenceCount == 0) {
						return;
					}
					referenceCount = referenceCountUpdater.decrementAndGet(this);
					if (referenceCount == 0) {
						debug.log("Closing '%s'", this.path);
						this.buffer = null;
						this.bufferPosition = -1;
						this.bufferSize = 0;
						this.fileChannel.close();
						tracker.closedFileChannel(this.path);
						this.fileChannel = null;
						if (this.randomAccessFile != null) {
							this.randomAccessFile.close();
							tracker.closedFileChannel(this.path);
							this.randomAccessFile = null;
						}
					}
				}
				finally {
					this.lock.unlock();
				}
			}
			debug.log("Reference count for '%s' decremented to %s", this.path, referenceCount);
		}

		/**
		 * Update the reference count without taking the lock, unless the update would
		 * need to open or close the channel.
		 * @param delta the amount to add
		 * @return the updated reference count or {@code 0} if the lock is needed
		 */
		private int updateReferenceCountWithoutLock(int delta) {
			int referenceCount = this.referenceCount;
			while (referenceCount > 0 && referenceCount + delta > 0) {
				if (referenceCountUpdater.compareAndSet(this, referenceCount, referenceCount + delta)) {
					return referenceCount + delta;
				}
				referenceCount = this.referenceCount;
			}
			return 0;
		}

		<E extends Exception> void ensureOpen(Supplier<E> exceptionSupplier) throws E {
			if (this.referenceCount == 0) {
				throw exceptionSupplier.get();
			}
		}

		@Override
		public String toString() {
			return this.path.toString();
		}

	}

	/**
	 * Internal tracker used to check open and closing of files in tests.
	 */
	interface Tracker {

		Tracker NONE = new Tracker() {

			@Override
			public void openedFileChannel(Path path) {
			}

			@Override
			public void closedFileChannel(Path path) {
			}

		};

		void openedFileChannel(Path path);

		void closedFileChannel(Path path);

	}

}
