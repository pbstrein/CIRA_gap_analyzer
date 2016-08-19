import argparse
from os.path import isdir
import sys
import json
import copy
import gapanalizer_functions as gapanalizer

# ------------------------------ Custom Exceptions ----------------------


class ProductError(Exception):
    def __init__(self, product_name, message):
        self.product_name = product_name
        self.message = message


class EpicRangeError(Exception):
    def __init__(self, message):
        self.message = message


class RecursiveDepthError(Exception):
    def __init__(self, message):
        self.message = message
# -------------------------------------MAIN-----------------------------------

# argparse commands -dbc, -ftc, -p, -e, -d , -v, -s|-ns, -rn|rl
parser = argparse.ArgumentParser()
parser.add_argument('-dbc', '--databaseconfig',
                    help='Location of the database config file')
parser.add_argument('-ftc', '--filetreeconfig',
                    help='Location of the file tree config file')
parser.add_argument('-p', '--product',
                    help='The root product that will be checked')
parser.add_argument('-e', '--epicrange', nargs='+', type=int,
                    help='The range of epics to checked')
parser.add_argument('-d', '--searchdepth', type=int,
                    help=('Depth of search starting from main product'))

parser.add_argument('-v', '--verbosity', action='count',
                    help=('Controls how much information outputs to the '
                          'console while running the program, max information '
                          'at -vvv'))

# mutally exclusive groups
summary_group = parser.add_mutually_exclusive_group()
summary_group.add_argument('-s', '--summaryname',
                           help='The name of the summary file')
summary_group.add_argument('-ns', '--nosummary', action='store_true', 
                           help='Turns off the printing of a summary file')
results_group = parser.add_mutually_exclusive_group()
results_group.add_argument('-nr', '--noresults', action='store_true',
                           help='Turns off the printing of results files')
results_group.add_argument('-rl', '--resultslocation',
                           help='The folder location of the results files')
args = parser.parse_args()


# default values, changed by command line arguments
product_name = '2B-GEOPROF'
epic_start = 0
epic_end = 6
search_depth = sys.getrecursionlimit()  # sets it ot the max depth that python allows
summary_name = ('results/' + product_name + '_E0' + str(epic_start) + ''
                '-E0' + str(epic_end) + '_summary')
no_summary = False
no_results = False
verbosity = 0
file_tree_config_loc = 'Config/input_config.json'
db_config_loc = 'Config/dbconfig.json'
results_loc = 'results/'


# ------------------------- Interpreting command line args -------------------

# no command line arguements, runs default values
if not len(sys.argv) > 1:
    print("Cannot run program --- need to specifiy a product and an epic range")
    print("Use -p [product_name] and -e [start] [end]")
    print("Type -h for more info on other commands")
    sys.exit()

if args.databaseconfig:  # -dbc
    db_config_loc = args.databaseconfig
if args.filetreeconfig:  # -ftc
    file_tree_config_loc = args.filetreeconfig

if(args.product):  # -p
    if(not args.epicrange):
        print("Cannot run program --- need an epic range")
        print("use -e [start] [end]")
        sys.exit()
    else:
        # creates the file trees to check if the product exists
        gapanalizer.create_file_inputs(file_tree_config_loc)
        if args.product not in gapanalizer.product_name_tree.keys():
            raise ProductError(args.product, 'Not a valid product name')
        else:
            product_name = args.product

if(args.epicrange):  # -e
    if not args.product:
        print("Cannot run program --- need to specific product")
        print("Use -p [product_name]")
        sys.exit()
    else:
        if len(args.epicrange) > 2:
            print('Too many inputs')
        # two arguments
        elif len(args.epicrange) == 2:
            # error checks the two arguments
            if args.epicrange[0] < 0 | args.epicrange[1] < 0:
                raise EpicRangeError(': Epic range must be positive')
            elif (args.epicrange[0] > 6) | (args.epicrange[1] > 6):
                raise EpicRangeError(": Epic range must be 6 or smaller")
            elif args.epicrange[0] > args.epicrange[1]:
                raise EpicRangeError(": Epic range must go from smaller to greater")
            # no errors, can continue and use the values
            else:
                epic_start = args.epicrange[0]
                epic_end = args.epicrange[1]
                summary_name = ('results/' + product_name + '_E0'
                                '' + str(epic_start) + '-E0' + str(epic_end) + ''
                                '_summary')
        # one argument
        elif len(args.epicrange) == 1:
            # error checks the one value
            if args.epicrange[0] < 0:
                raise EpicRangeError(': Epic range must be positive')
            elif args.epicrange[0] > 6:
                raise EpicRangeError(": Epic range must be 6 or smaller")
            # no errors, can use the value
            else:
                epic_start = args.epicrange[0]
                epic_end = args.epicrange[0]
                summary_name = ('results/' + product_name + '_E0'
                                '' + str(epic_start) + '_summary')

if(args.searchdepth):  # -d
    # checks if negative number
    if(args.searchdepth < 0):
        raise RecursiveDepthError("Cannot be a negative number")
    else:
        search_depth = args.searchdepth

if(args.summaryname):  # -s
    if args.summaryname.find('/') != -1:
        s = args.summaryname.split('/')
        new_string = ''
        for i in range(len(s) - 1):
            new_string += s[i] + '/'
        if not isdir(new_string):
            raise FileNotFoundError(new_string + " was not found")
    summary_name = args.summaryname
if(args.nosummary):  # -ns
    no_summary = args.nosummary  # sets no_summary to true

if args.resultslocation:  # -rl
    if not isdir(args.resultslocation):
        raise FileNotFoundError(args.resultslocation + " was not found")
    else:
        results_loc = args.resultslocation
if args.noresults:  # -nr
    no_results = args.noresults

if args.verbosity:  # -v; sets verbosity, if any
    verbosity = args.verbosity

# prints program specificatoins of level 1 or more verbosity
if verbosity >= 1:
    print()
    print("\tProgram Specifications:")
    print("\tFile Tree config location:", file_tree_config_loc)
    print("\tDatabase Config location:", db_config_loc)
    print("\tProduct:", product_name)
    print("\tEpic Start:", epic_start)
    print("\tEpic End:", epic_end)
    print("\tSearch Depth:", search_depth)
    if not no_summary:
        print("\tSummary File Name:", summary_name)
    else:
        print("\tSummary printing off")
    if not no_results:
        print("\tResults files location:", results_loc)
    else:
        print("\tResults files printing off")
    print()
# runs main function with values given - values changed based on defaluts
# and/or if they were any commandline arguments
gapanalizer.main(product_name, epic_start, epic_end, summary_name, no_summary,
                 search_depth, verbosity, file_tree_config_loc, db_config_loc,
                 results_loc, no_results)
