# btc-classifier
Python scripts suite for generating Bitcoin transactions graphs and analyzing them using machine learning.

# Overview
The solution architecture high-level view:

![Solution architecture](https://github.com/aggstam/btc-classifier/blob/main/images/Solution%20Architecture.png)

## parser.py 
This script is a modified version of Blockchain parser by Denis Leonov [1].<br>
Script parses blk*.dat files of the Bitcoin blockchain and produces files containing simplyfied transactions,
using the btcpy library [2].

![Simplyfied BTC transaction](https://github.com/aggstam/btc-classifier/blob/main/images/Simplified_Bitcoin_Transaction_Example_corrected.png)

Using parser.py script, the Bitcoin blockchain was parsed until file blk02399.dat, sizing 298 GB in total.<br>
All blk*.dat files were parsed after 60 hours.<br>
Output file size averaged at 180 MB, with a total size of 426 GB.

## reader.py
Script parses the output files of parser.py script and imports retrieved information to the Database.<br>
This streamline method was chosen, in order to simplify parsing and importing steps, and also enabling batch execution.<br>
All output files of parser.py script were parsed after 9 days 2 hours 32 minutes and 29 seconds, 
resulting in 652 GB of disk size for the Database using row compression.<br>
To further increase Database performance, field indexing can be enabled,
along with the modification of MySQL configuration parameter innodb_buffer_pool_size,
which can be set to 16 GB, to increase the Database RAM cache size, for faster query executions.<br>
You can use provided database_schema_creation.sql to create the following schema:

![Database schema](https://github.com/aggstam/btc-classifier/blob/main/images/Database_Schema.png)

## transactions_retriever.py
Imported data are processed by transactions_retriever.py script, which generates the execution dataset for the analyzer.py script.<br>
A random address sample is retrieved from the Entity-address dataset for 2010-2018 Bitcoin transactions [3].<br>
For each address in the sample, all their transaction ids are retrieved from the Database, to create the execution dataset output files.

## analyzer.py
This script performs an unsupervised Machine Learning task, 
using Deep Graph Infomax [4] and Graph Convolutional Network (GCN) [5] algorithms for node representation learning.<br>
After node features have been extracted, classification of each node on the temporal network graph
for the Bitcoin transactions dataset is executed, using Logistic regression.<br>
StellarGraph [6] libraries are used for the Machine Learning tasks.<br>
On each execution, a dedicated folder is created containing all result files.<br>
Results files include each fold predictions, the graphs .graphml file and a loss over epoch diagram.

![Generated .graphml file](https://github.com/aggstam/btc-classifier/blob/main/images/analyzer_generate_graph_example.png)

![Generated history file](https://github.com/aggstam/btc-classifier/blob/main/images/analyzer_deep_graph_infomax_plot.png)

# Execution
Before executing any script, install required dependencies:
```
% pip install -r requirements.txt
```

!IMPORTANT:

stellargraph requires Python >=3.6.0, <3.8.0 so make sure you have that python versions installed, along with their pip module.<br>
Example requirements installation for python3.8, while system uses python3.10
```
% python3.8 -m ensurepip --upgrade
% python3.8 -m pip install --user -r requirements.txt
```
MySQL connector for python must also be installed.<br>
Link: https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html

All scripts can be executed by using their name:
```
% python analyzer.py
```

# Configuration:
This section describes all the configuration needed for scripts execution.<br>
Please configure all values appropriately before execution.

## parser.py
| Line | Name | Description                      |
|------|------|----------------------------------|
|  94  | dirA | path to Bitcoin blk files folder |
|  95  | dirB | script output folder             |

## reader.py
| Line | Name        | Description                    |
|------|-------------|--------------------------------|
|  70  | host        | MySQL host                     |
|  71  | user        | MySQL user                     |
|  72  | password    | MySQL user password            |
|  73  | database    | MySQL database name            |
| 151  | dir         | parser.py script output folder |
| 152  | start_index | parse from blk number          |
| 153  | end_index   | parse until blk number         |

## transactions_retrieve.py
| Line  | Name                  | Description                       |
|-------|-----------------------|-----------------------------------|
| 33-38 | *_ADDRESSES_FILE      | path to each address file dataset |
|  42   | TXIN_QUERY.timestamp  | tx timestamp max value            |
|  43   | TXOUT_QUERY.timestamp | tx timestamp max value            |
| 46-52 | *_CSV_FILE            | script output csv files           |
|  70   | host                  | MySQL host                        |
|  71   | user                  | MySQL user                        |
|  72   | password              | MySQL user password               |
|  73   | database              | MySQL database name               |

## analyzer.py
| Line  | Name          | Description                                      |
|-------|---------------|--------------------------------------------------|
|  55   | OUTPUT_FOLDER | script output folder                             |
| 56-62 | *_CSV_FILE    | transactions_retrieve.py script output csv files |
|  69   | FOLDS         | K-Fold validation k parameter                    |
|  70   | EPOCHS        | ML training epochs                               |

# References:
[1] Blockchain parser: https://github.com/ragestack/blockchain-parser<br>
[2] btcpy: https://github.com/chainside/btcpy<br>
[3] Original dataset: https://github.com/Maru92/EntityAddressBitcoin<br>
[4] Veličković, P., Fedus, W., Hamilton, W. L., Lio, P., Bengio, Y., Hjelm, R. D., (2019). Deep Graph Infomax. ICLR, arXiv:1809.10341.<br>
[5] Kipf, T. N., Max Welling, (2017). Graph Convolutional Networks (GCN): Semi-Supervised Classification with Graph Convolutional Networks. International Conference on Learning Representations (ICLR).<br>
[6] StellarGraph: https://github.com/stellargraph/stellargraph
