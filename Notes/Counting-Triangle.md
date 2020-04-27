# Clustering Coefficient 聚集系数的计算


# Naive algorithm

![alt text](./images/parallel-version.png)

- cc(Blue) = N/A
- cc(Red) = 1/3
- cc(Green) = 1
- cc(Black) = 1

## Data structure
### Raw data
数字表示结点，一对数字，表示这两个节点之间有一条边。

## 用OOD其实不高效?
https://javadeveloperzone.com/hadoop/hadoop-create-custom-key-writable-example/

## Mapper 0
- emit(<Red, Blue>)
- emit(<Red, Black>)
- emit(<Red, Green>)
- emit(<Blue, Red>)
- emit(<Black, Red>)
- emit(<Black, Green>)
- emit(<Green, Red>)
- emit(<Green, Black>)

## Reducer 0
- Input
    1. <Red, [Blue, Black, Green]>
    2. <Blue, [Red]>
    3. <Black, [Red, Green]>
    4. <Green, [Red, Black]>
- Output
    1. <(actual edge), "$">         
    2. <(possible edge), vertex>    // nested loop

## Mapper 1
- Input
    - 0,1     $
    - 1,3     $
    - 1,2     $
    - 1,0     $
    - 3,2     1
    - 3,0     1
    - 2,0     1
    - 2,3     $
    - 2,1     $
    - 3,1     2
    - 3,2     $
    - 3,1     $
    - 2,1     3

- Output: combine all the edges as primary keys.


### Reducer 1
- Input: <(v1, v2), [u1, u2, ... , uk]> or <(v1, v2), [u1, u2, ... , uk, $]>

```
flag = false    // a boolean variable. True if (v1,v2) is an actual edge
for each value in values: 
    if value is "$":
        flag = true

int size = values.length
if flag is true:
    for ( i = 0; i < size; i++ )
        for ( j = i + 1; j < size; j++ ) 
            emit(value, 1)      
    
```

## Mapper 2
- Input: <vertex, 1>
- emit<vertex, 1>

## Reducer2
- Input: <vertex, [1, 1, 1, ... , 1]>

```
int count = 0
for each value in values: 
    count += value

emit(vertex, value)

```



