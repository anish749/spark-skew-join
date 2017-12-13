# spark-skew-join
Fragment Replicate Join for Spark Data frames using Count Min Sketches for estimating skews.

## What is Skew and how can we handle skew?
The Map Reduce paradigm as well as the way Spark handles data processing has the underlying pattern of “bringing all records with the same key to the same place”. This creates hot keys in cases where records pertaining to one key are significantly more than others.

Collecting hot keys in a single reducer can lead to significant skew, that is, one reducer that must process significantly more records than the others. Since a MapReduce job is only complete when all of its mappers and reducers have completed, any subsequent jobs must wait for the slowest reducer to complete before they can start.

This library provides an algorithm for compensating the skew. Much like the skewed join method in Pig, this library first runs a sampling job to determine which keys are hot. When performing the actual join, the mappers send any records relating to a hot key to one of several reducers, chosen at random (in contrast to conventional MapReduce, which chooses a reducer deterministically based on a hash of the key). For the other input to the join, records relating to the hot key need to be replicated to all reducers handling that key.

This technique spreads the work of handling the hot key over several reducers, which allows it to be parallelized better, at the cost of having to replicate the other join input to multiple reducers.

-- The above explanation is taken from Designing Data Intensive Applications, Chap 10 by Martin Kleppmann

## Using the library
Currently this provides APIs in Scala.

```
import org.anish.spark.skew.dfimplicits._
val skewJoined = left_df.skewJoin(right_df, Array("join_col1", "join_col2"), joinType = "inner", SkewJoinConf(skewType = LeftSkew)) // Defines that the skew is in the left data frame.
```
In the above example the left data frame is sampled to find the data values which have the skew and is then fragmented. The right data frame values are replicated accordingly and then the join is executed.

## TODO
- Publish as a package that can be imported.
- Add more examples.
- Add API for Datasets

