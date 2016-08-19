# Data Gap Analyzer

### Summary
The Data Gap Analyzer goes through the files in the CloudSat database and finds the files missing from a user-given product, and finds the cause of why the file is missing, whether it was missing a primary input, or one of its parent files had an algorithm error and was not created, thus making the child product not appear. It takes all the information of the missing files and prints them into result files and prints a summary of results to a summary file.

### Program Requirements
To run, this program needs 3 things
1. A json file that contains the config information to connect to the database 
  - **Note**: By default the program searches for the file at `config/dbconfig.json`, to change that, use `-dbc [file_loc]` in the command line
2. A json file that contains the input file tree, meaning, what “parent” products are required to make a “child” format. 
  - It must be in the format of ‘[child_product_name]:[_CS_[parent_product_name]_GRANULE_P_R04_E02.hdf]` 
    - if the parent product is 0A-CPR or 1A-AUX, then it must end with `.CPR` or `.1AA` respectively instead of .hdf
  - by default it finds the file at `config/input_config.json`, to change the location, use `-ftc [file_loc]` in the command line
  - *Note*: the epic does not matter, the program will change the epic as it goes along
3. In the command line, `python3 gapanalizer.py -p [product_name] -e [epic_number]`
  - You cannot use gapanalizer_functions, it will not work, only gapanalizer.py
  - `-p [product_name]` selects the child/base product with name `[product_name]` that will be searched
  - `-e [epic_number]` will select the epic that will be searched
    - For example: `-e 2 4` will will run through the epics of E02 - E04
  - *Note* - there is no difference in functionality if using `python`, only the results will print more `()`, `,`, and `’’`

#### Example
‘python3 gapanalizer.py -p 2B-GEOPROF -e 2 6`
Using python3, will check the products of 2B-GEOPROF from E02 - E06 and find the missing gaps and why.
- **Note** - you can use `python` (python 2.7) but the program will run just fine, just the result texts will include more `()`, `,`, and `’’`

### Output
Two kinda of files will be made by running this program:
1. Results file by epic - text files that show all the files missing from an orbit, and the conclusion as to why there were missing files - missing primary input (0A-CPR or 1A-AUX) or algorithm error of some file.
  - These files are found by default in a folder called `results`, but that folder can be changed by using `-rl [foldername]`
2. Summary file of program - A summary of the results of all the epics. Contains the execution time, the total files check, total files missing, child products missing, and child products checked. Also contains the number of missing primary input errors and algorithm errors by product
  - **Note** - the child product files missing and the total number of errors are not always the same
  - **Note** - by default the summary is called `results/[product_name]_[epic range]_summary`, but can be changed by `-s [filename]`

### Tips and Warnings
1. Use `-v` to increase the verbosity of the console. It can also help with debugging if a problem arises. Maximum console output at `-vvv`
2. If the file tree config is not made correctly, it will screw up the whole program and result in bad results - make sure that it is made correctly with key value as product name and value as a list of abbreviated parent filenames
  - For Example
```
{“0A-CPR”: [],
  “1B-CPR”: ["_CS_0A-CPR_GRANULE_P_R04_E00.CPR", 
  	     	"_CS_1A-AUX_GRANULE_P_R04_E00.1AA"]
  "MODIS-AUX": ["_CS_1B-CPR_GRANULE_P_R04_E02.hdf”]
}
```
3. Be sure to include 0A-CPR and 1A-AUX in the file tree, but leave their values as blank lists
  - ie - `“0A-CPR”: []`
4. When using `-e`, you can have a single number to get a single epic, or you can use two numbers to get a range
  - **i.e.** - `-e 2` will run E02
  - **i.e.** - `-e 2 4` will run E02 - E04
  - **NOTE** - the range must be smaller to greater, `-e 4 2` will not work
5. You can turn off printing of the results or summary through `-ns` or `-nr`

### Command Line Arguments
- `-h` Command line help
- `-dbc [dbconfig_file_loc]` - if you don’t want to use the default location of dbconfig file, use this command to specify a specific location
- `-ftc [filetreeconfig_file_loc]` - used to specificy a different location than the default for the file_tree json file
- `-p [product_name]` - REQUIRED - the child product that will be checked for gaps
- `-e [epic_number]` OR `-e [lower_limit] [upper_limit]` - the epic range that will be checked in the program
- `-d [depth]` - how deep of a search the program will run on the child product, 1 will only check the childs parents, and no more, 2 will run up to child’s grandparents, etc. 
- `-v` - Changes how much information is displayed in the output during program execution.
- `-s [filename]` - Name of summary file you don’t want to use the default
- `-ns` - turns off the printing of the summary
- `-nr` - turns off the printing of the results file
- `-rl` [foldername] - location of the folder where the results files will be printed if the default does not want to be used

### Default values
```
- depth = 10000 (python’s max recursion rate)
- summary_printing = on
- results_print = on
- verbosity = 0
- file tree config location = Config/input_config.json
- database config location = Config/dbconfig.json
- results folder location = results/
```
