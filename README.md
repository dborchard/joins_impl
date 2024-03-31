## Joins Implementation.

### Algorithms

Three fundamental algorithms for performing a binary join operation exist:

- hash join
    - Grace Hash Join [DONE]
    - Naive Main Memory Hash Join
    - [External Hash Table](https://www.youtube.com/watch?v=ev3UHuDIiDg&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=6) [DONE]
- sort-merge join [DONE]
    - [Double Buffering](https://www.youtube.com/watch?v=qdeBmEnv_bI&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=2)
    - [External Merge Sort](https://www.youtube.com/watch?v=hRgrnQU-uJ4&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=4)
    - [Sort vs Hash: Pros and Cons](https://www.youtube.com/watch?v=5TvveH6bgEo&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=11)
- symmetric hash join (streaming) [DONE]
  - Join Schema which [doesn't have Pipeline Breakers](https://www.youtube.com/watch?v=jveohy_qhHU&list=PLzzVuDSjP25QY8TJcPLh7WqP81qYk9A0m&index=9)
  - Send output as soon as you get input rather than waiting for the whole join to end.
  - There is a Out-of-core Symmetric Hash Join (X-Join Paper). Not used in practical systems.
- nested loop join
  
### Core Ideas

- [Double Buffering](https://www.youtube.com/watch?v=qdeBmEnv_bI&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=2): [CoW Btree Page 50](https://schd.ws/hosted_files/buildstuff14/96/20141120-BuildStuff-Lightning.pdf),
  External Algorithms
- [Sampling for Partitioning/Sorting](https://www.youtube.com/watch?v=zDj72vqypks&list=PLzzVuDSjP25QY8TJcPLh7WqP81qYk9A0m&index=7):
  Used in parallel merge join, IVF Flat using
  K-means, [Distributed KD Tree](https://medium.com/sys-base/spatial-partitioned-rdd-using-kd-tree-in-spark-102e0b53564b)
- [Hashing vs Sorting](https://www.youtube.com/watch?v=Pm58kIR4EQE&list=PLzzVuDSjP25Qpsaf7GxFDBEWwvQKCkCVl&index=12)
  - Hashing is Divide, Hash, and Manage
  - Sorting is Divide, Sort, and Merge

### Good Reference Videos

- [Grace Hash Joins 1](https://www.youtube.com/watch?v=SYJJxmoLVIY&list=PLzzVuDSjP25RQb_VhEBFWFiB7oS9APM7h&index=9)
- [Grace Hash Joins 2](https://www.youtube.com/watch?v=gQaMmO757Eo&list=PLzzVuDSjP25RQb_VhEBFWFiB7oS9APM7h&index=10)
- [Reference Projects](https://github.com/dimitraka71/advdbHashJoin)
- https://en.wikipedia.org/wiki/Hash_join
- https://en.wikipedia.org/wiki/Nested_loop_join
- https://en.wikipedia.org/wiki/Sort-merge_join
- https://cgi.cse.unsw.edu.au/~cs9315/21T1/lectures/join-hash/slides.html#s2
- https://pages.cs.wisc.edu/~travitch/notes/cs764-notes-midterm1.pdf
- [Loop Join Animation](https://notes.bencuan.me/cs186/05-iterators-and-joins/)
- https://faculty.cc.gatech.edu/~jarulraj/courses/4420-f20/slides/20-joins.pdf
- https://courses.cs.washington.edu/courses/cse444/10au/lectures/lecture20.pdf
- https://pages.cs.wisc.edu/~yxy/cs764-f21/slides/L2.pdf


### Need to revisit
- https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2
- https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
- https://www.cockroachlabs.com/blog/vectorizing-the-merge-joiner-in-cockroachdb/