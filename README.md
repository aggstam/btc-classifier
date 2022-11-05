# btc-classifier
Python scripts suite for generating Bitcoin transactions graphs and analyzing them using machine learning.

# Execution
Before executing any script, install required dependencies:
```
% pip install -r requirements.txt
```

All scripts can be executed by using their name:
```
% python analyzer.py
```

# Configuration:
This section describes all the configuration needed for scripts execution.
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
