
### Difference between Map and FaltMap in PySpark

map() – Spark map() transformation applies a function to each row in a DataFrame/Dataset and returns the new transformed Dataset.Function in map can return only one item.
flatMap() – Spark flatMap() transformation flattens the DataFrame/Dataset after applying the function on every element and returns a new transformed Dataset but output is flattened. 
The returned Dataset will return more rows than the current DataFrame. It is also referred to as a one-to-many(flatMap can return a list of elements (0 or more)) transformation function.

| Map        | FlatMap           | 
| ------------- |:-------------:| 
| * Spark map() transformation applies a function to each row in a DataFrame/Dataset and returns the new transformed Dataset.
  * Function in map can return only one item.      | * Spark flatMap() transformation flattens the DataFrame/Dataset after applying the function on every element and returns a new transformed Dataset but output is flattened. 
                                                      * The returned Dataset will return more rows than the current DataFrame. 
                                                      * It is also referred to as a one-to-many(flatMap can return a list of elements (0 or             more)) transformation function. | 
 
 
                                                    
                                                                                                 
