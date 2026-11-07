/*
 * Copyright 2012-present the original author or authors.
 */

package org.springframework.boot.loader.zip;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.assertj.core.extractor.Extractors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.boot.loader.zip.FileDataBlock.FileAccess;
import org.springframework.boot.loader.zip.FileDataBlock.Tracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/**
 * Tests for {@link FileDataBlock}.
 *
 * @author Phillip Webb
 */
class FileDataBlockTests {

	private static final byte[] CONTENT = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05 };

	@TempDir
	File tempDir;

	File tempFile;

	@BeforeEach
	void writeTempFile() throws IOException {
		this.tempFile = new File(this.tempDir, "content");
		Files.write(this.tempFile.toPath(), CONTENT);
	}

	@AfterEach
	void resetTracker() {
		FileDataBlock.tracker = Tracker.NONE;
	}

	@Test
	void sizeReturnsFileSize() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			assertThat(block.size()).isEqualTo(CONTENT.length);
		}
	}

	@Test
	void readReadsFile() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
			assertThat(block.read(buffer, 0)).isEqualTo(6);
			assertThat(buffer.array()).containsExactly(CONTENT);
		}
	}

	@Test
	void readReadsFileWhenThreadHasBeenInterrupted() throws IOException {
		Files.write(this.tempFile.toPath(), CONTENT);
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
			Thread.currentThread().interrupt();
			assertThat(block.read(buffer, 0)).isEqualTo(6);
			assertThat(buffer.array()).containsExactly(CONTENT);
		}
		finally {
			Thread.interrupted();
		}
	}

	@Test
	void readDoesNotReadPastEndOfFile() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
			assertThat(block.read(buffer, 2)).isEqualTo(4);
			assertThat(buffer.array()).containsExactly(0x02, 0x03, 0x04, 0x05, 0x0, 0x0);
		}
	}

	@Test
	void readWhenPosAtSizeReturnsMinusOne() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
			assertThat(block.read(buffer, 6)).isEqualTo(-1);
		}
	}

	@Test
	void readWhenPosOverSizeReturnsMinusOne() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
			assertThat(block.read(buffer, 7)).isEqualTo(-1);
		}
	}

	@Test
	void readWhenPosIsNegativeThrowsException() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
			assertThatIllegalArgumentException().isThrownBy(() -> block.read(buffer, -1));
		}
	}

	@Test
	void sliceWhenOffsetIsNegativeThrowsException() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			assertThatIllegalArgumentException().isThrownBy(() -> block.slice(-1, 0))
				.withMessage("Offset must not be negative");
		}
	}

	@Test
	void sliceWhenSizeIsNegativeThrowsException() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			assertThatIllegalArgumentException().isThrownBy(() -> block.slice(0, -1))
				.withMessage("Size must not be negative and must be within bounds");
		}
	}

	@Test
	void sliceWhenSizeIsOutOfBoundsThrowsException() throws IOException {
		try (FileDataBlock block = createAndOpenBlock()) {
			assertThatIllegalArgumentException().isThrownBy(() -> block.slice(2, 5))
				.withMessage("Size must not be negative and must be within bounds");
		}
	}

	@Test
	void sliceReturnsSlice() throws IOException {
		try (FileDataBlock slice = createAndOpenBlock().slice(1, 4)) {
			assertThat(slice.size()).isEqualTo(4);
			ByteBuffer buffer = ByteBuffer.allocate(4);
			assertThat(slice.read(buffer, 0)).isEqualTo(4);
			assertThat(buffer.array()).containsExactly(0x01, 0x02, 0x03, 0x04);
		}
	}

	@Test
	void openAndCloseHandleReferenceCounting() throws IOException {
		TestTracker tracker = new TestTracker();
		FileDataBlock.tracker = tracker;
		FileDataBlock block = createBlock();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(0);
		tracker.assertOpenCloseCounts(0, 0);
		block.open();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(1);
		tracker.assertOpenCloseCounts(1, 0);
		block.open();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(2);
		tracker.assertOpenCloseCounts(1, 0);
		block.close();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(1);
		tracker.assertOpenCloseCounts(1, 0);
		block.close();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(0);
		tracker.assertOpenCloseCounts(1, 1);
		block.open();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(1);
		tracker.assertOpenCloseCounts(2, 1);
		block.close();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(0);
		tracker.assertOpenCloseCounts(2, 2);
	}

	@Test
	void openAndCloseSliceHandleReferenceCounting() throws IOException {
		TestTracker tracker = new TestTracker();
		FileDataBlock.tracker = tracker;
		FileDataBlock block = createBlock();
		FileDataBlock slice = block.slice(1, 4);
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(0);
		tracker.assertOpenCloseCounts(0, 0);
		block.open();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(1);
		tracker.assertOpenCloseCounts(1, 0);
		slice.open();
		assertThat(slice).extracting("fileAccess.referenceCount").isEqualTo(2);
		tracker.assertOpenCloseCounts(1, 0);
		slice.open();
		assertThat(slice).extracting("fileAccess.referenceCount").isEqualTo(3);
		tracker.assertOpenCloseCounts(1, 0);
		slice.close();
		assertThat(slice).extracting("fileAccess.referenceCount").isEqualTo(2);
		tracker.assertOpenCloseCounts(1, 0);
		slice.close();
		assertThat(slice).extracting("fileAccess.referenceCount").isEqualTo(1);
		tracker.assertOpenCloseCounts(1, 0);
		block.close();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(0);
		tracker.assertOpenCloseCounts(1, 1);
		slice.open();
		assertThat(slice).extracting("fileAccess.referenceCount").isEqualTo(1);
		tracker.assertOpenCloseCounts(2, 1);
		slice.close();
		assertThat(slice).extracting("fileAccess.referenceCount").isEqualTo(0);
		tracker.assertOpenCloseCounts(2, 2);
	}

	@Test
	void readWhenChannelHasBeenClosedByInterruptOfAnotherThreadReopensChannel() throws IOException {
		byte[] content = createContent(FileAccess.BUFFER_SIZE * 3);
		Files.write(this.tempFile.toPath(), content);
		TestTracker tracker = new TestTracker();
		FileDataBlock.tracker = tracker;
		try (FileDataBlock block = createAndOpenBlock()) {
			ByteBuffer buffer = ByteBuffer.allocate(16);
			block.readFully(buffer, 0);
			getFileChannel(block).close();
			buffer.clear();
			int pos = FileAccess.BUFFER_SIZE * 2;
			block.readFully(buffer, pos);
			assertThat(buffer.array()).containsExactly(Arrays.copyOfRange(content, pos, pos + 16));
			assertThat(getFileChannel(block).isOpen()).isTrue();
			tracker.assertOpenCloseCounts(2, 1);
		}
		tracker.assertOpenCloseCounts(2, 2);
	}

	@Test
	void readWhenLockIsHeldByAnotherThreadReadsFromChannel() throws Exception {
		ExecutorService lockExecutor = Executors.newSingleThreadExecutor();
		ExecutorService readExecutor = Executors.newSingleThreadExecutor();
		try (FileDataBlock block = createAndOpenBlock()) {
			ReentrantLock lock = getLock(block);
			lockExecutor.submit(lock::lock).get();
			try {
				ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
				Future<Integer> read = readExecutor.submit(() -> block.read(buffer, 1));
				assertThat(read.get(30, TimeUnit.SECONDS)).isEqualTo(5);
				assertThat(buffer.array()).containsExactly(0x01, 0x02, 0x03, 0x04, 0x05, 0x00);
			}
			finally {
				lockExecutor.submit(lock::unlock).get();
			}
		}
		finally {
			lockExecutor.shutdown();
			readExecutor.shutdown();
		}
	}

	@Test
	void readWhenLockIsHeldByAnotherThreadAndChannelHasBeenClosedWaitsForLock() throws Exception {
		ExecutorService lockExecutor = Executors.newSingleThreadExecutor();
		ExecutorService readExecutor = Executors.newSingleThreadExecutor();
		try (FileDataBlock block = createAndOpenBlock()) {
			ReentrantLock lock = getLock(block);
			lockExecutor.submit(lock::lock).get();
			Future<Integer> read;
			try {
				getFileChannel(block).close();
				read = readExecutor.submit(() -> block.read(ByteBuffer.allocate(CONTENT.length), 0));
				await().atMost(Duration.ofSeconds(30)).until(lock::hasQueuedThreads);
				assertThat(read).isNotDone();
			}
			finally {
				lockExecutor.submit(lock::unlock).get();
			}
			assertThat(read.get(30, TimeUnit.SECONDS)).isEqualTo(6);
			assertThat(getFileChannel(block).isOpen()).isTrue();
		}
		finally {
			lockExecutor.shutdown();
			readExecutor.shutdown();
		}
	}

	@Test
	void readWhenReadingThreadsAreInterruptedReturnsContent() throws Exception {
		byte[] content = createContent(FileAccess.BUFFER_SIZE * 20);
		Files.write(this.tempFile.toPath(), content);
		TestTracker tracker = new TestTracker();
		FileDataBlock.tracker = tracker;
		FileDataBlock block = createAndOpenBlock();
		Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			Random random = new Random(i);
			threads.add(new Thread(() -> {
				try {
					ByteBuffer buffer = ByteBuffer.allocate(512);
					for (int j = 0; j < 2000; j++) {
						int pos = random.nextInt(content.length - buffer.capacity());
						buffer.clear();
						block.readFully(buffer, pos);
						Thread.interrupted();
						assertThat(buffer.array()).isEqualTo(Arrays.copyOfRange(content, pos, pos + buffer.capacity()));
					}
				}
				catch (Throwable ex) {
					failures.add(ex);
				}
			}));
		}
		threads.forEach(Thread::start);
		Random random = new Random();
		while (threads.stream().anyMatch(Thread::isAlive)) {
			threads.get(random.nextInt(threads.size())).interrupt();
			Thread.onSpinWait();
		}
		for (Thread thread : threads) {
			thread.join();
		}
		block.close();
		assertThat(failures).isEmpty();
		assertThat(tracker.openCount).isEqualTo(tracker.closeCount);
	}

	@Test
	void openAndCloseFromMultipleThreadsHandleReferenceCounting() throws Exception {
		TestTracker tracker = new TestTracker();
		FileDataBlock.tracker = tracker;
		FileDataBlock block = createBlock();
		Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			threads.add(new Thread(() -> {
				try {
					for (int j = 0; j < 5000; j++) {
						block.open();
						try {
							ByteBuffer buffer = ByteBuffer.allocate(CONTENT.length);
							assertThat(block.read(buffer, 0)).isEqualTo(CONTENT.length);
						}
						finally {
							block.close();
						}
					}
				}
				catch (Throwable ex) {
					failures.add(ex);
				}
			}));
		}
		threads.forEach(Thread::start);
		for (Thread thread : threads) {
			thread.join();
		}
		assertThat(failures).isEmpty();
		assertThat(block).extracting("fileAccess.referenceCount").isEqualTo(0);
		assertThat(getFileChannel(block)).isNull();
		assertThat(tracker.openCount).isEqualTo(tracker.closeCount);
	}

	private FileChannel getFileChannel(FileDataBlock block) {
		return (FileChannel) Extractors.byName("fileAccess.fileChannel").apply(block);
	}

	private ReentrantLock getLock(FileDataBlock block) {
		return (ReentrantLock) Extractors.byName("fileAccess.lock").apply(block);
	}

	private byte[] createContent(int size) {
		byte[] content = new byte[size];
		new Random(0).nextBytes(content);
		return content;
	}

	private FileDataBlock createAndOpenBlock() throws IOException {
		FileDataBlock block = createBlock();
		block.open();
		return block;
	}

	private FileDataBlock createBlock() throws IOException {
		return new FileDataBlock(this.tempFile.toPath());
	}

	static class TestTracker implements Tracker {

		private int openCount;

		private int closeCount;

		@Override
		public void openedFileChannel(Path path) {
			this.openCount++;
		}

		@Override
		public void closedFileChannel(Path path) {
			this.closeCount++;
		}

		void assertOpenCloseCounts(int expectedOpenCount, int expectedCloseCount) {
			assertThat(this.openCount).as("openCount").isEqualTo(expectedOpenCount);
			assertThat(this.closeCount).as("closeCount").isEqualTo(expectedCloseCount);
		}

	}

}
