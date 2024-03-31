## Joins Implementation.

### Algorithms

Three fundamental algorithms for performing a binary join operation exist:

- hash join
    - Grace Hash Join [DONE]
    - Naive Main Memory Hash Join
    - [External Hash Table](https://www.youtube.com/watch?v=ev3UHuDIiDg&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=6) [DONE]
- nested loop join
- sort-merge join
    - [Double Buffering](https://www.youtube.com/watch?v=qdeBmEnv_bI&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=2)
    - [External Merge Sort](https://www.youtube.com/watch?v=hRgrnQU-uJ4&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=4)
- symmetric hash join (streaming)

### Core Ideas

- [Double Buffering](https://www.youtube.com/watch?v=qdeBmEnv_bI&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=2): [CoW Btree Page 50](https://schd.ws/hosted_files/buildstuff14/96/20141120-BuildStuff-Lightning.pdf),
  External Algorithms
- [Sampling for Partitioning/Sorting](https://www.youtube.com/watch?v=zDj72vqypks&list=PLzzVuDSjP25QY8TJcPLh7WqP81qYk9A0m&index=7):
  Used in parallel merge join, IVF Flat using
  K-means, [Distributed KD Tree](https://medium.com/sys-base/spatial-partitioned-rdd-using-kd-tree-in-spark-102e0b53564b)

### Good Reference Videos

- [Grace Hash Joins 1](https://www.youtube.com/watch?v=SYJJxmoLVIY&list=PLzzVuDSjP25RQb_VhEBFWFiB7oS9APM7h&index=9)
- [Grace Hash Joins 2](https://www.youtube.com/watch?v=gQaMmO757Eo&list=PLzzVuDSjP25RQb_VhEBFWFiB7oS9APM7h&index=10)
- [Reference Projects](https://github.com/dimitraka71/advdbHashJoin)
- https://en.wikipedia.org/wiki/Hash_join
- https://en.wikipedia.org/wiki/Nested_loop_join
- https://en.wikipedia.org/wiki/Sort-merge_join