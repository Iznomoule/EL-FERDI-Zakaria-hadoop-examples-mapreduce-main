# YARN & MapReduce2

### EL FERDI Zakaria DE1 - Big Data Frameworks
### 18/07/2024

## 1.8 Remarkable tress of Paris

```
login as: zakaria.el-ferdi
zakaria.el-ferdi@bigdata01.efrei.hadoop.clemlab.io's password:
Last login: Tue Jul 16 09:40:26 2024 from 89.39.107.193
[zakaria.el-ferdi@bigdata01 ~]$ kinit zakaria.el-ferdi                          Password for zakaria.el-ferdi@EFREI.HADOOP.CLEMLAB.IO:
[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -ls
Found 12 items
drwx------   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-16 02:00 .Trash
drwx------   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-16 10:16 .staging
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-12 16:16 data
-rw-r--r--   3 zakaria.el-ferdi zakaria.el-ferdi    3359405 2024-07-12 15:15 ebook.txt
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-12 17:05 gutenberg
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-12 17:17 gutenberg-output
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-15 15:45 input_dir
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-16 10:16 output
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-15 15:49 output_dir
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-11 11:49 raw
-rw-r--r--   3 zakaria.el-ferdi zakaria.el-ferdi      16680 2024-07-15 18:22 trees.csv
drwxr-xr-x   - zakaria.el-ferdi zakaria.el-ferdi          0 2024-07-12 15:17 wordcount
```

AppDriver.java
```
package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class,
                    "A map/reduce program that lists the distinct districts containing trees.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists the different species of trees.");
            programDriver.addClass("treeskind", Treeskind.class,
                    "A map/reduce program that counts the number of trees of each kind.");
            programDriver.addClass("maxheightkind", Maxheightkind.class,
                    "A map/reduce program that calculates the maximum height of each kind of tree.");
            programDriver.addClass("sorttrees", SortTrees.class,
                    "A map/reduce program that sorts trees by height from smallest to largest.");
            programDriver.addClass("oldesttreedistrict", OldestTreeDistrict.class,
                    "A map/reduce program that finds the district with the oldest tree.");
            programDriver.addClass("districtmaxtrees", DistrictMaxTrees.class,
                    "A map/reduce program that finds the district with the most trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```


### 1.8.1 District containing trees

DistrictMpper.java
```
package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            String[] fields = value.toString().split(";");
            context.write(new IntWritable(Integer.parseInt(fields[1])), new IntWritable(1));
        }
        rowCounter++;
    }
}
```

DistrictReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable val : values) {
            total += val.get();
        }
        result.set(total);
        context.write(key, result);
    }
}
```

District.java
```
package com.opstty.job;

import com.opstty.mapper.DistrictMapper;
import com.opstty.reducer.DistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class District {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: distinct Districts <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "district");
        job.setJarByClass(District.class);
        job.setMapperClass(DistrictMapper.class);
        job.setCombinerClass(DistrictReducer.class);
        job.setReducerClass(DistrictReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```


### Result
```
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar district /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/district

[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/district/part-r-00000
3       1
4       1
5       2
6       1
7       3
8       5
9       1
11      1
12      29
13      2
14      3
15      1
16      36
17      1
18      1
19      6
20      3
```

### 1.8.2 Show all existing species

SpeciesMapper.java
```
package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            String[] fields = value.toString().split(";");
            context.write(new Text(fields[3]), NullWritable.get());
        }
        rowCounter++;
    }
}
```

SpeciesReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeciesReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
```

Species.java
```
package com.opstty.job;

import com.opstty.mapper.SpeciesMapper;
import com.opstty.reducer.SpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Species {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Trees species <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "species");
        job.setJarByClass(Species.class);
        job.setMapperClass(SpeciesMapper.class);
        job.setCombinerClass(SpeciesReducer.class);
        job.setReducerClass(SpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Result
``` 
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar species /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/species

[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/species/part-r-00000
araucana
atlantica
australis
baccata
bignonioides
biloba
bungeana
cappadocicum
carpinifolia
colurna
coulteri
decurrens
dioicus
distichum
excelsior
fraxinifolia
giganteum
giraldii
glutinosa
grandiflora
hippocastanum
ilex
involucrata
japonicum
kaki
libanii
monspessulanum
nigra
nigra laricio
opalus
orientalis
papyrifera
petraea
pomifera
pseudoacacia
sempervirens
serrata
stenoptera
suber
sylvatica
tomentosa
tulipifera
ulmoides
virginiana
x acerifolia
```

### 1.8.3 Number of trees by kinds

TreeskindMapper.java
```
package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeskindMapper extends Mapper<Object, Text, Text, IntWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            String[] fields = value.toString().split(";");
            context.write(new Text(fields[3]), new IntWritable(1));
        }
        rowCounter++;
    }
}
```

TreeskindReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TreeskindReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
```

Treeskind.java
```
package com.opstty.job;

import com.opstty.mapper.TreeskindMapper;
import com.opstty.reducer.TreeskindReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Treeskind {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Number trees by kind <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "treeskind");
        job.setJarByClass(Treeskind.class);
        job.setMapperClass(TreeskindMapper.class);
        job.setCombinerClass(TreeskindReducer.class);
        job.setReducerClass(TreeskindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Result
```
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar treeskind /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/treeskind


[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/treeskind/part-r-00000
araucana        1
atlantica       2
australis       1
baccata 2
bignonioides    1
biloba  5
bungeana        1
cappadocicum    1
carpinifolia    4
colurna 3
coulteri        1
decurrens       1
dioicus 1
distichum       3
excelsior       1
fraxinifolia    2
giganteum       5
giraldii        1
glutinosa       1
grandiflora     1
hippocastanum   3
ilex    1
involucrata     1
japonicum       1
kaki    2
libanii 2
monspessulanum  1
nigra   3
nigra laricio   1
opalus  1
orientalis      8
papyrifera      1
petraea 2
pomifera        1
pseudoacacia    1
sempervirens    1
serrata 1
stenoptera      1
suber   1
sylvatica       8
tomentosa       2
tulipifera      2
ulmoides        1
virginiana      2
x acerifolia    11
```

### 1.8.4 Maximum height per kind of tree

MaxHeightKindMapper.java
```
package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightKindMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            try {
                String[] fields = value.toString().split(";");
                Float height = Float.parseFloat(fields[6]);
                context.write(new Text(fields[3]), new FloatWritable(height));
            } catch (NumberFormatException e) {
                // Ignore parsing errors
            }
        }
        rowCounter++;
    }
}
```

MaxHeightKindReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MaxHeightKindReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        List<Float> heights = StreamSupport.stream(values.spliterator(), false)
                .map(FloatWritable::get)
                .collect(Collectors.toList());
        Float maxHeight = Collections.max(heights);
        context.write(key, new FloatWritable(maxHeight));
    }
}
```

MaxHeightKind.java
```
package com.opstty.job;

import com.opstty.mapper.MaxHeightKindMapper;
import com.opstty.reducer.MaxHeightKindReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Maxheightkind {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Max Height per kind <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "maxheightkind");
        job.setJarByClass(Maxheightkind.class);
        job.setMapperClass(MaxHeightKindMapper.class);
        job.setCombinerClass(MaxHeightKindReducer.class);
        job.setReducerClass(MaxHeightKindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Result
```
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar maxheightkind /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/maxheightkind

[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/maxheightkind/part-r-00000
araucana        9.0
atlantica       25.0
australis       16.0
baccata 13.0
bignonioides    15.0
biloba  33.0
bungeana        10.0
cappadocicum    16.0
carpinifolia    30.0
colurna 20.0
coulteri        14.0
decurrens       20.0
dioicus 10.0
distichum       35.0
excelsior       30.0
fraxinifolia    27.0
giganteum       35.0
giraldii        35.0
glutinosa       16.0
grandiflora     12.0
hippocastanum   30.0
ilex    15.0
involucrata     12.0
japonicum       10.0
kaki    14.0
libanii 30.0
monspessulanum  12.0
nigra   30.0
nigra laricio   30.0
opalus  15.0
orientalis      34.0
papyrifera      12.0
petraea 31.0
pomifera        13.0
pseudoacacia    11.0
sempervirens    30.0
serrata 18.0
stenoptera      30.0
suber   10.0
sylvatica       30.0
tomentosa       20.0
tulipifera      35.0
ulmoides        12.0
virginiana      14.0
x acerifolia    45.0
```

### 1.8.5 Sort the trees height from smallest to largest

SortTreesMapper.java
```
package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortTreesMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            try {
                String[] parts = value.toString().split(";");
                Float height = Float.parseFloat(parts[6]);
                String outputValue = parts[11] + " - " + parts[2] + " " + parts[3] + " (" + parts[4] + ")";
                context.write(new FloatWritable(height), new Text(outputValue));
            } catch (NumberFormatException e) {
                // Ignore parsing errors
            }
        }
        rowCounter++;
    }
}
```

SortTreesReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortTreesReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    public void reduce(FloatWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
```

SortTrees.java
```
package com.opstty.job;

import com.opstty.mapper.SortTreesMapper;
import com.opstty.reducer.SortTreesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Sort Trees Height from Smallest to Largest <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sortTrees");
        job.setJarByClass(SortTrees.class);
        job.setMapperClass(SortTreesMapper.class);
        job.setReducerClass(SortTreesReducer.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```

### Result
```
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar sorttrees /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/sorttrees

[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/sorttrees/part-r-00000
3 - Fagus sylvatica (Fagaceae)  2.0
89 - Taxus baccata (Taxaceae)   5.0
62 - Cedrus atlantica (Pinaceae)        6.0
39 - Araucaria araucana (Araucariaceae) 9.0
44 - Styphnolobium japonicum (Fabaceae) 10.0
32 - Quercus suber (Fagaceae)   10.0
95 - Pinus bungeana (Pinaceae)  10.0
61 - Gymnocladus dioicus (Fabaceae)     10.0
63 - Fagus sylvatica (Fagaceae) 10.0
4 - Robinia pseudoacacia (Fabaceae)     11.0
93 - Diospyros virginiana (Ebenaceae)   12.0
66 - Magnolia grandiflora (Magnoliaceae)        12.0
50 - Zelkova carpinifolia (Ulmaceae)    12.0
7 - Eucommia ulmoides (Eucomiaceae)     12.0
48 - Acer monspessulanum (Sapindacaees) 12.0
58 - Diospyros kaki (Ebenaceae) 12.0
33 - Broussonetia papyrifera (Moraceae) 12.0
71 - Davidia involucrata (Cornaceae)    12.0
36 - Taxus baccata (Taxaceae)   13.0
6 - Maclura pomifera (Moraceae) 13.0
68 - Diospyros kaki (Ebenaceae) 14.0
96 - Pinus coulteri (Pinaceae)  14.0
94 - Diospyros virginiana (Ebenaceae)   14.0
91 - Acer opalus (Sapindaceae)  15.0
5 - Catalpa bignonioides (Bignoniaceae) 15.0
70 - Fagus sylvatica (Fagaceae) 15.0
2 - Ulmus carpinifolia (Ulmaceae)       15.0
98 - Quercus ilex (Fagaceae)    15.0
28 - Alnus glutinosa (Betulaceae)       16.0
78 - Acer cappadocicum (Sapindaceae)    16.0
75 - Zelkova carpinifolia (Ulmaceae)    16.0
16 - Celtis australis (Cannabaceae)     16.0
64 - Ginkgo biloba (Ginkgoaceae)        18.0
83 - Zelkova serrata (Ulmaceae) 18.0
23 - Aesculus hippocastanum (Sapindaceae)       18.0
60 - Fagus sylvatica (Fagaceae) 18.0
34 - Corylus colurna (Betulaceae)       20.0
51 - Platanus x acerifolia (Platanaceae)        20.0
43 - Tilia tomentosa (Malvaceae)        20.0
15 - Corylus colurna (Betulaceae)       20.0
11 - Calocedrus decurrens (Cupressaceae)        20.0
1 - Corylus colurna (Betulaceae)        20.0
8 - Platanus orientalis (Platanaceae)   20.0
20 - Fagus sylvatica (Fagaceae) 20.0
35 - Paulownia tomentosa (Paulowniaceae)        20.0
12 - Sequoiadendron giganteum (Taxodiaceae)     20.0
87 - Taxodium distichum (Taxodiaceae)   20.0
13 - Platanus orientalis (Platanaceae)  20.0
10 - Ginkgo biloba (Ginkgoaceae)        22.0
47 - Aesculus hippocastanum (Sapindaceae)       22.0
86 - Platanus orientalis (Platanaceae)  22.0
14 - Pterocarya fraxinifolia (Juglandaceae)     22.0
88 - Liriodendron tulipifera (Magnoliaceae)     22.0
18 - Fagus sylvatica (Fagaceae) 23.0
24 - Cedrus atlantica (Pinaceae)        25.0
31 - Ginkgo biloba (Ginkgoaceae)        25.0
92 - Platanus x acerifolia (Platanaceae)        25.0
49 - Platanus orientalis (Platanaceae)  25.0
97 - Pinus nigra (Pinaceae)     25.0
84 - Ginkgo biloba (Ginkgoaceae)        25.0
73 - Platanus orientalis (Platanaceae)  26.0
65 - Pterocarya fraxinifolia (Juglandaceae)     27.0
42 - Platanus orientalis (Platanaceae)  27.0
85 - Juglans nigra (Juglandaceae)       28.0
76 - Pinus nigra laricio (Pinaceae)     30.0
19 - Quercus petraea (Fagaceae) 30.0
72 - Sequoiadendron giganteum (Taxodiaceae)     30.0
54 - Pterocarya stenoptera (Juglandaceae)       30.0
29 - Zelkova carpinifolia (Ulmaceae)    30.0
27 - Sequoia sempervirens (Taxodiaceae) 30.0
25 - Fagus sylvatica (Fagaceae) 30.0
41 - Platanus x acerifolia (Platanaceae)        30.0
77 - Taxodium distichum (Taxodiaceae)   30.0
55 - Platanus x acerifolia (Platanaceae)        30.0
69 - Pinus nigra (Pinaceae)     30.0
38 - Fagus sylvatica (Fagaceae) 30.0
59 - Sequoiadendron giganteum (Taxodiaceae)     30.0
52 - Fraxinus excelsior (Oleaceae)      30.0
37 - Cedrus libanii (Pinaceae)  30.0
22 - Cedrus libanii (Pinaceae)  30.0
30 - Aesculus hippocastanum (Sapindaceae)       30.0
80 - Quercus petraea (Fagaceae) 31.0
9 - Platanus orientalis (Platanaceae)   31.0
82 - Platanus x acerifolia (Platanaceae)        32.0
46 - Ginkgo biloba (Ginkgoaceae)        33.0
45 - Platanus orientalis (Platanaceae)  34.0
56 - Taxodium distichum (Taxodiaceae)   35.0
81 - Liriodendron tulipifera (Magnoliaceae)     35.0
17 - Platanus x acerifolia (Platanaceae)        35.0
53 - Ailanthus giraldii (Simaroubaceae) 35.0
57 - Sequoiadendron giganteum (Taxodiaceae)     35.0
26 - Platanus x acerifolia (Platanaceae)        40.0
74 - Platanus x acerifolia (Platanaceae)        40.0
40 - Platanus x acerifolia (Platanaceae)        40.0
90 - Platanus x acerifolia (Platanaceae)        42.0
21 - Platanus x acerifolia (Platanaceae)        45.0
```

### 1.8.6 District containing the oldest tree

OldestTreeDistrictMapper.java
```
package com.opstty.mapper;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeDistrictMapper extends Mapper<Object, Text, NullWritable, MapWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            try {
                String[] fields = value.toString().split(";");
                Integer year = Integer.parseInt(fields[5]);
                MapWritable map = new MapWritable();
                map.put(new IntWritable(Integer.parseInt(fields[1])), new IntWritable(year));
                context.write(NullWritable.get(), map);
            } catch (NumberFormatException e) {
                // Ignore parsing errors
            }
        }
        rowCounter++;
    }
}
```

OldestTreeDistrictReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OldestTreeDistrictReducer extends Reducer<NullWritable, MapWritable, IntWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        List<int[]> districtYears = StreamSupport.stream(values.spliterator(), false)
                .map(mw -> new int[]{
                        ((IntWritable) mw.keySet().iterator().next()).get(),
                        ((IntWritable) mw.get(mw.keySet().iterator().next())).get()
                })
                .collect(Collectors.toList());

        int oldestYear = districtYears.stream().mapToInt(arr -> arr[1]).min().orElse(Integer.MAX_VALUE);

        districtYears.stream()
                .filter(arr -> arr[1] == oldestYear)
                .forEach(arr -> {
                    try {
                        context.write(new IntWritable(arr[0]), new IntWritable(oldestYear));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}

```

OldestTreeDistrict.java
```
package com.opstty.job;

import com.opstty.mapper.OldestTreeDistrictMapper;
import com.opstty.reducer.OldestTreeDistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OldestTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: OldestTreeDistrict <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "oldestTreeDistrict");
        job.setJarByClass(OldestTreeDistrict.class);
        job.setMapperClass(OldestTreeDistrictMapper.class);
        job.setReducerClass(OldestTreeDistrictReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```


```
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar oldesttreedistrict /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/oldesttreedistrict

[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/oldesttreedistrict/part-r-00000
5       1601

```



### 1.8.7 District containing the most trees

DistrictMaxTreesMapper.java
```
package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMaxTreesMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private int lineNumber = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (lineNumber != 0) {
            String[] parts = value.toString().split(";");
            context.write(new IntWritable(Integer.parseInt(parts[1])), new IntWritable(1));
        }
        lineNumber++;
    }
}
```

DistrictMaxTreesReducer.java
```
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistrictMaxTreesReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private List<int[]> districtCounts = new ArrayList<>();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        districtCounts.add(new int[]{key.get(), count});
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int maxTrees = districtCounts.stream().mapToInt(arr -> arr[1]).max().orElse(0);
        districtCounts.stream()
                .filter(arr -> arr[1] == maxTrees)
                .forEach(arr -> {
                    try {
                        context.write(new IntWritable(arr[0]), new IntWritable(maxTrees));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}
```

DistrictMaxTrees.java
```
package com.opstty.job;

import com.opstty.mapper.DistrictMaxTreesMapper;
import com.opstty.reducer.DistrictMaxTreesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictMaxTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Districtmaxtrees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "districtmaxtrees");
        job.setJarByClass(DistrictMaxTrees.class);
        job.setMapperClass(DistrictMaxTreesMapper.class);
        job.setReducerClass(DistrictMaxTreesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Result
```
> hadoop jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar districtmaxtrees /user/zakaria.el-ferdi/trees.csv /user/zakaria.el-ferdi/output/districtmaxtrees

[zakaria.el-ferdi@bigdata01 ~]$ hdfs dfs -cat /user/zakaria.el-ferdi/output/districtmaxtrees/part-r-00000
16      36
```