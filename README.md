# bufferstuff
Utilities for working with NIO Buffers. Including a BitSet implementation and sorting algos.

To add a dependency on bufferstuff using Maven, use the following:

```xml
<dependency>
  <groupId>tech.bitey</groupId>
  <artifactId>bufferstuff</artifactId>
  <version>0.5.0</version>
</dependency>
```

## Functionality
### tech.bitey.bufferstuff.BufferBitSet
Similar to java.util.BitSet, but backed by a ByteBuffer. Differences with BitSet include:

BufferBitSet
- ... is not Serializable
- ... does not hide the backing buffer, and offers copy-free methods for wrapping an existing buffer
- ... allows for specifying whether or not the buffer can be resized (replaced with a larger buffer)
```java
BufferBitSet bbs = new BufferBitSet();
bbs.set(0, 5);
bbs.clear(2);
System.out.println(bbs); // [0, 1, 3, 4]
```
```java
byte[] b = new byte[] {(byte)0, (byte)7};
BufferBitSet bbs = new BufferBitSet(ByteBuffer.wrap(b));
System.out.println(bbs); // [8, 9, 10]
bbs.clear(9);
System.out.println(bbs); // [8, 10]
System.out.println(Arrays.toString(b)); // [0, 5]
```
Note: use the `tech.bitey.allocateDirect` system propery to control whether buffers created by BufferBitSet are direct/off-heap (if property is set to "true"), or array-backed/on-heap. Defaults to array-backed/on-heap.

### tech.bitey.bufferstuff.BufferSort
- various sorting algorithms, including heap sort as the in-place `O(n*log(n))` worst-case sorting alogirthm, as well as various linear time sorts
- the generic "sort" method chooses among insertion, heap, and linear time sorts, depending on the length of the range to be sorted
```java
int[] array = new int[] {100, 4, 1, 3, 5, 2, -100};
IntBuffer b = IntBuffer.wrap(array);
BufferSort.sort(b, 1, 6);
System.out.println(Arrays.toString(array)); // [100, 1, 2, 3, 4, 5, -100]
```

### tech.bitey.bufferstuff.BufferSearch
- binary search on sorted buffers
- also supports quickly finding the first/last element in a sequence of duplicates
```java
int[] array = new int[] {7, 7, 8, 8, 8, 8, 8, 9, 9};
IntBuffer b = IntBuffer.wrap(array);
int index = BufferSearch.binarySearch(b, 0, b.limit(), 8);
System.out.println(index); // 4
index = BufferSearch.binaryFindFirst(b, 0, index);
System.out.println(index); // 2
```

### tech.bitey.bufferstuff.BufferUtils

- miscellaneous utility methods for working with buffers
  * variants of ByteBuffer duplicate()/slice()/asReadOnlyBuffer() which preserve byte order
  * test if a buffer is sorted, or sorted and distinct
  * deduplicate a sorted buffer
  * copy a buffer
  * etc.
