from os import listdir
from os.path import isfile, join
from fnmatch import fnmatch
import copy
import time
import datetime
import sys
import json


import mysql.connector
from mysql.connector import errorcode

# --------------------------GLOBAL VARIABLES------------------------------

# left blank to be global - are given value from create_file_inputs function
filename_tree = {

}
product_name_tree = {

}

# -----------------------------Classes-------------------------------


class RecursionFunctionHolder():
    """ Holds all the data necesary to pass into the gap finder recursive
        function

        attrib:
        missing_file_obj - the DataFile object that is missing/ in a gap
        files_to_search  - a set containing all the files that will be
                           checked - uses a set instead of queries to the
                           db to be faster
        sum_info         - A Summary object that will contain all the info
                           for the summary at the end of the main func
        files_searched   - A set that will contain all the product names
                           that have already been searched in the recursive
                           function to reduce repeating searched trees
        conclusion_str   - a string that will output the conclusion,
                           either primary input or algorithm error
    """
    def __init__(self, missing_file_obj, search_set, sum_info,
                 recursion_level_max):
        self.missing_file_obj = missing_file_obj
        self.files_to_search = search_set
        self.sum_info = sum_info
        self.files_searched = set()
        self.conclusion_str = []
        self.recursion_level_max = recursion_level_max


class Summary():
    """
    Contains all the info that will be used by the summary at the end of the
    main

    total_files_checked - all files checked in the recursive funtion
    total_files_missing - number of files missing in the entire search
    main_product_files_checked - number of files checked of the initial
                                 product checked
    main_product_files_missing - files missing from initial product checked
    missing_prikmary_input_error - all instances of the primary input error
    algorithm error - all instances of algorithm error
    upstream_input-error - all instances of when primary input exists, but
                           other input files are missing necessary to make
                           the inital product
    main_product_error - all instances of algorithm error only on initial
                         product
    product_errors - dict of product errors
                    - where keys are the product
                    - and the value is the number of errors
    start_time - start time of the program
    end_time - time when the program finishes
    """
    def __init__(self, dict_keys_input_list):
        self.total_files_checked = 1
        self.total_files_missing = 0
        self.main_product_files_checked = 1
        self.main_product_files_missing = 0
        self.missing_primary_input_error = 0
        self.algorithm_errors = 0
        self.upstream_input_error = 0
        self.main_product_error = 0
        self.product_errors = self.create_product_dict(dict_keys_input_list)
        self.start_time = ''
        self.end_time = ''
        self.execution_time = 0

    def create_product_dict(self, dict_keys):
        """ Creates the list of the all the products that will be checked
         """
        new_dict = {}
        for keys in dict_keys:
            new_dict[keys] = 0
        return new_dict

    def set_all_error_totals(self, primary_input_name_1, primary_input_name_2,
                             main_product_name):
        """
        sets the values of both primary_input and algorithm errors

        requires knowing the name of the two primary inputs that have no
        parents, and the main product being checked
        """
        self.missing_primary_input_error = self.product_errors[primary_input_name_1]
        for key in self.product_errors:
            if((key != primary_input_name_1) & (key != primary_input_name_2) & (key != main_product_name)):
                self.upstream_input_error += self.product_errors[key]
        self.main_product_error = self.product_errors[main_product_name]
        self.algorithm_errors = self.upstream_input_error + self.main_product_error


class DataFile():
    """
    Contains all the information of a the data file and changing the name

    filename - filename of the data,
               format [****]
    timestamp - timestamp of file
    orbit - orbit of file
    product_name - name of the product
    end_name - end string of filename
    """
    def __init__(self, filename):
        temp = filename.split('_')
        self.filename = filename
        self.timestamp = temp[0]
        self.orbit = int(temp[1])
        self.product_name = temp[3]
        self.end_name = filename[filename.find('_GRANULE'):]

    def remake_filename(self):
        """
        Remakes the filename and replaces timestamp with wildcard *
        """
        """
        code to make filename here
        """

    def change_orbit(self, new_orbit):
        """ Changes orbit to the new_orbit, and remakes filename"""
        self.orbit = new_orbit
        self.remake_filename()

    def change_name(self, new_name):
        """ CHanges name to new_name and remakes filename"""
        self.product_name = new_name
        self.remake_filename()

    def get_filename_wildcard(self):
        """ Returns the filename as a wildcardname from the timestamp"""
        index = self.filename.find('_')
        wildcard_name = '*' + self.filename[index:]
        return wildcard_name


class DBConnector():
    """Contains all the info to make a connection to the database, and contain
       the variables for a connection

       Attributes
       db_info - all of the login information needed to get into a database;
                 user, password host, port, and database
       cnx - myql.connecotr connection to database, set in make_connections()
       cursor - myql.cursor to make queries into the database, set in
                make_connections()
       make_connections() - sets cnx and cursor to the db_info, and  tries
                            connection to database - if it cant connect or
                            connection breaks, retries 5 times, then quits the
                            program if it does not connect
        close_connections - officially closes cnx and cursor to finish
                            connection to the database """

    def __init__(self, db_info):
        self.db_info = db_info
        self.make_connection()
        # cnx and cursor are public variables, but are set in make_connections,
        # not in __init__

    def make_connection(self):
        connection_tries = 0
        delay_time = 5  # reconnection time in seconds
        while (connection_tries < 5):
            try:
                cnx = mysql.connector.connect(**self.db_info)
                cursor = cnx.cursor()
            except mysql.connector.Error as err:
                # wrong hostname
                if err.errno == errorcode.CR_WRONG_HOST_INFO:
                    print(err)
                    # break
                # could not make initial server connection
                elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                    print("Could not make initial connection. Retrying in 5",
                          "seconds...")
                # errors during the a query
                elif((err.errno == errorcode.CR_SERVER_LOST_EXTENDED) |
                     (err.errno == errorcode.CR_SERVER_LOST) |
                     (err.errno == errorcode.ER_SERVER_SHUTDOWN)):
                    print(err)
                    print("Database connection broke. Reconnecting...")
                    cursor.close()
                # errors in database access informatoin (password, name, etc.)
                elif ((err.errno == errorcode.ER_DBACCESS_DENIED_ERROR) |
                      (err.errno == errorcode.ER_ACCESS_DENIED_ERROR)):
                    print(err)
                # unknown or uncaught error
                else:
                    raise
                # wait for retry
                connection_tries += 1
                time.sleep(delay_time)
            # retrieve info from query and close query when there are no
            # connections problems
            else:
                self.cnx = cnx
                self.cursor = cursor
                break
        # failed to connect after retrying multiple times
        else:
            print("Could not reconnect at all. Exiting program")
            sys.exit()

    def close_connections(self):
        self.cursor.close()
        self.cnx.close()


# ---------------------------Definations---------------------------

def run_sql_command(DBConnector, sql_command):
    """Runs the given sql command, and handles for a database disconnect. If
    cannot connect to database, returns false. If connects, executes
    command, and returns values if select, or none if another command
    """

    connection_tries = 0
    delay_time = 5  # reconnection time in seconds
    while (connection_tries < 5):
        try:
            DBConnector.cursor.execute(sql_command)
        except mysql.connector.Error as err:
            # wrong hostname
            if err.errno == errorcode.CR_WRONG_HOST_INFO:
                print(err)
                # break
            # could not make initial server connection
            elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("Could not make initial connection. Retrying in 5",
                      "seconds...")
            # errors during the a query
            elif((err.errno == errorcode.CR_SERVER_LOST_EXTENDED) |
                 (err.errno == errorcode.CR_SERVER_LOST) |
                 (err.errno == errorcode.ER_SERVER_SHUTDOWN)):
                print(err)
                print("Database connection broke. Reconnecting...")
                DBConnector.cursor.close()
            # errors in database access informatoin (password, name, etc.)
            elif ((err.errno == errorcode.ER_DBACCESS_DENIED_ERROR) |
                  (err.errno == errorcode.ER_ACCESS_DENIED_ERROR)):
                print(err)
            # unknown or uncaught error
            else:
                raise
            # wait for retry
            connection_tries += 1
            time.sleep(delay_time)

            #  retry connection
            DBConnector.make_connection()
        # retrieve info from query and close query when there are no
        # connections problems
        else:
            rows = []
            if(DBConnector.cursor.with_rows):
                rows = DBConnector.cursor.fetchall()
            DBConnector.cnx.commit()
            return rows
    # failed to connect after retrying multiple times
    else:
        print("Could not reconnect at all. Exiting program")
        sys.exit()


def get_files(DBConnector, tb_name, product_name):
    """
    Returns a tuple of the files in the DBConnector database based on the
    table name and product_name
    """
    search_wildcard = '%' + product_name
    query = ("SELECT filename FROM {} "
             "WHERE filename LIKE {!r}") .format(tb_name, search_wildcard)
    query_list = run_sql_command(DBConnector, query)
    return query_list


def create_filename_list_from_location(location):
    """creates a list of all the file names in the given directory - excludes
    folders"""
    file_list = [f for f in listdir(location) if fnmatch(f, '*_GRANULE*')]
    return file_list


def create_file_inputs(input_json_loc):
    """ Creates the file trees from a given json file location"""
    # accesses module global variables
    global filename_tree
    global product_name_tree

    # creates dictionaries from json file
    f = open(input_json_loc, 'r')
    with open(input_json_loc) as f:
        dict1 = json.load(f)
    # creates product name dictionary from the file name dictionary by
    # splitting and finding the product name in the filename
    dict2 = copy.deepcopy(dict1)
    for keys in dict2:
        for i in range(len(dict2[keys])):
            my_list = dict2[keys][i].split('_')
            dict2[keys][i] = my_list[2]
    # assigns global variables to from local vars
    filename_tree = dict1
    product_name_tree = dict2


def write_summary(main_product_name, output_text_name, summary_info):
    """
    Writes the summary of the what happened in the gap check
    """
    with open(output_text_name, 'w') as summary_file:
        summary_file.write(('Total Execution time: '
                            '' + str(summary_info.execution_time) + '\n'))
        summary_file.write('Start time: ' + summary_info.start_time + '\n')
        summary_file.write('End time: ' + summary_info.end_time + '\n')
        summary_file.write('\n')

        summary_file.write('Total files checked: '
                           + str(summary_info.total_files_checked) + '\n')
        summary_file.write('Total files missing: '
                           + str(summary_info.total_files_missing) + '\n')
        summary_file.write('Main files checked: '
                           + str(summary_info.main_product_files_checked)
                           + '\n')
        summary_file.write('Main files missing: '
                           + str(summary_info.main_product_files_missing)
                           + '\n')
        summary_file.write('\n')

        summary_file.write('Total Missing Primary Input Errors: ' 
                           + str(summary_info.missing_primary_input_error) 
                           + '\n')
        summary_file.write('Total Algorithm Errors: ' 
                           + str(summary_info.algorithm_errors) + '\n')
        summary_file.write('\tUpstream Input Errors: ' 
                           + str(summary_info.upstream_input_error) + '\n')
        for keys in sorted(summary_info.product_errors.keys()):
            if((keys != main_product_name) & (keys != '0A-CPR')
                    & (keys != '1A-AUX')):
                if(summary_info.product_errors[keys] != 0):
                    summary_file.write('\t\t' + keys + ' files missing: '
                                       + str(summary_info.product_errors[keys])
                                       + '\n')
        summary_file.write('\tMain Product Errors: ' 
                           + str(summary_info.main_product_error) + '\n')
        summary_file.write('\t\t' + main_product_name + ' files missing: ' 
                           + str(summary_info.product_errors[main_product_name])
                           + '\n')


def create_files_set(DBConnector, product_name, search_list, verbosity):
    """
    Recursively creates a set of all the files based on the product given,
    based on its inputs
    """
    # stops from duplicating searches
    if product_name in search_list:
        return search_list
    # prints if user comandlines -vvv
    if(verbosity >= 3):
        print("Retrieving", product_name)
    for i in range(len(product_name_tree[product_name])):
        # recursively gets all input files
        search_list = create_files_set(DBConnector, product_name_tree[product_name][i], search_list, verbosity)
        # reduces duplicating searches
        if(product_name_tree[product_name][i] in search_list):
            continue
        # make set of all db files
        search_list[product_name_tree[product_name][i]] = set()
        temp_list = get_files(DBConnector, 'files',
                              filename_tree[product_name][i])
        for j in range(len(temp_list)):
            index = temp_list[j][0].find('_')
            search_list[product_name_tree[product_name][i]].update([temp_list[j][0][index+1:]])
        if(verbosity >= 3):
            print("\t Retrieved", filename_tree[product_name][i])
    return search_list


def find_parents_missing_files(recursion_data, cur_level):
    """Recursively seaches a through a file and its parents to find the missing
       files
    Returns a string with the output of which files are missing and a
    conclusion of whether it was an algorithm error or prmiary input error

    attributes:
    recursion_data - a RecursionFunctionHolder object that contains all
                     the informatoin necessary to accomplish the recursion
                     (missing file obj, summary info, set of files, files
                     searched, and the conclusion string)
   """
    output_str = ''
    algorithm_err = True
    # file has already been searched once, returns blank
    if (recursion_data.missing_file_obj.product_name in recursion_data.files_searched):
        return output_str
    # file has no parents (ie 0A-CPR or 1A-AUX), ends recursion
    if not(filename_tree[recursion_data.missing_file_obj.product_name]):
        output_str = 'MISSING ' + recursion_data.missing_file_obj.get_filename_wildcard()
        output_str += '\n'
        recursion_data.conclusion_str.append('CONCLUSION: MISSING PRIMARY INPUT ERROR')
        recursion_data.sum_info.product_errors[recursion_data.missing_file_obj.product_name] += 1
        return output_str

    # loop through all the child's parents, and checks their files
    for i in range(len(product_name_tree[recursion_data.missing_file_obj.product_name])):

        recursion_data.sum_info.total_files_checked += 1
        recursion_data.files_searched.update([recursion_data.missing_file_obj.product_name])
        # opens parent file list
        # creates the orbit-ildcard name of the file to be searched to see
        # if it is missing
        search_word = '{0:05d}'.format(recursion_data.missing_file_obj.orbit) +'_' + filename_tree[recursion_data.missing_file_obj.product_name][i][1:]

        if (search_word in recursion_data.files_to_search[product_name_tree[recursion_data.missing_file_obj.product_name][i]]):
            pass

        # file was not found, this parent has a missing file
        else:
            # create new file_obj to be used in another recursive search
            recursion_data.sum_info.total_files_missing += 1
            algorithm_err = False
            new_missing_file_obj = copy.copy(recursion_data.missing_file_obj)
            new_missing_file_name = product_name_tree[recursion_data.missing_file_obj.product_name][i]
            new_missing_file_obj.change_name(new_missing_file_name)
            old_missing_file_obj = recursion_data.missing_file_obj
            recursion_data.missing_file_obj = new_missing_file_obj
            # recursively search new missing files branch to see which of its
            # parents are missing files
            if(cur_level < recursion_data.recursion_level_max):
                temp_str = find_parents_missing_files(recursion_data,
                                                      cur_level + 1)
                output_str += temp_str
            recursion_data.missing_file_obj = old_missing_file_obj
    # searched all parents, and since we know the current file is a missing has
    # a missing file, we include that file in string
    else:
        output_str += 'MISSING ' + recursion_data.missing_file_obj.get_filename_wildcard()
        output_str += '\n'
    # returns files found missing - if nothing was found, returns blank string
    if algorithm_err:
        recursion_data.sum_info.product_errors[recursion_data.missing_file_obj.product_name] += 1
        recursion_data.conclusion_str.append("CONCLUSION: ALGORITHM ERROR for " + recursion_data.missing_file_obj.product_name)
        # print(recursion_data.missing_file_obj.orbit)
        # print(recursion_data.sum_info.product_errors)
        # print(recursion_data.missing_file_obj.product_name)
    return output_str


# --------------------------MAIN------------------------------
def main(product_to_check, epic_start, epic_end, summary_name, no_summary,
         max_recurs_level, verbosity, input_config_loc, db_config_loc,
         results_folder_name, no_results):
    # retrieves all the config information to access database
    with open(db_config_loc) as file:
        db_config = json.load(file)

    # creates file tree if not already created
    if (not filename_tree) | (not product_name_tree):
        create_file_inputs(input_config_loc)

    # begins summary and marks program start time
    summary_info = Summary(product_name_tree.keys())
    summary_info.start_time = time.strftime('%H:%M:%S')

    # connects to db
    input_cnx = DBConnector(db_config)

    # loops for all the epics given from command line
    for epic in range(epic_start, epic_end+1):
        etag = 'E0' + str(epic)
        for key in filename_tree:
            # skips 0A-CPR and 1A-AUX b/c epic doesnt change for them
            if key == '1B-CPR':
                pass
            else:
                # renames filename so that it includes the new epic
                for j in range(len(filename_tree[key])):
                    index = filename_tree[key][j].find('E0')
                    end_index = filename_tree[key][j].find('.')
                    new_name = (filename_tree[key][j][:index] + etag
                                + filename_tree[key][j][end_index:])
                    filename_tree[key][j] = new_name
        # creates filename for main product given by commandline

        sql_product = ''  # new name of product goes here
        if(verbosity >= 2):
            print("Retreiving files for ", sql_product)
        #  retrieves a list of the main files, and sorts to get correct results
        file_list = get_files(input_cnx, 'files', sql_product)
        file_list.sort()

        # skips if no files found for epic
        if not file_list:
            print("No files for ", sql_product, "found, moving on")
            continue

        if(verbosity >= 2):
            print("Retrieving all the input from the database")
        # creates set of all files that will be checked to run quicker than
        # querying during search
        search_set = {}
        search_set = create_files_set(input_cnx, product_to_check, search_set,
                                      verbosity)
        if(verbosity >= 2):
            print("Finding gaps")
        # opens result file for writing during gap check
        if (not no_results):
            file = open(results_folder_name + product_to_check + '_' + etag
                        + '_results', 'w')
        # begins gap search by setting up current file
        current_file = DataFile(file_list[0][0])
        for i in range(1, len(file_list)):

            # writes to summary that product is checked
            summary_info.main_product_files_checked += 1
            summary_info.total_files_checked += 1

            # moves files to prepare for gap check between files
            prev_file = current_file
            current_file = DataFile(file_list[i][0])

            # giets difference between files
            orbit_dif = current_file.orbit - prev_file.orbit
            if(orbit_dif > 1):  # gap if there is a orbit dif is greater than one
                # copy to prepare for recursive check
                missing_file = copy.copy(prev_file)
                for j in range(1, orbit_dif):  # goes through all missing orbits
                    # increments missing main products in summary
                    summary_info.main_product_files_missing += 1
                    summary_info.total_files_checked += 1

                    # make new file with missing orbit
                    missing_orbit = prev_file.orbit + j
                    # make new file with missing orbit
                    missing_file.change_orbit(missing_orbit)

                    # prepare for recursion
                    recursion_func_holder = RecursionFunctionHolder(
                                                missing_file, search_set,
                                                summary_info, max_recurs_level)
                    # recursively finds all missing files, and recieves a string
                    # that has all the missing files
                    s = find_parents_missing_files(recursion_func_holder, 0)
                    # does not print if -nr in commandline
                    if not no_results:
                        file.write(s)  # writes all missing files found to
                                       # results file
                        for f in range(len(recursion_func_holder.conclusion_str)):
                            # writes all conclusions (if multiple) to results file
                            file.write(recursion_func_holder.conclusion_str[f] + '\n')
                        file.write('\n')
        # does not close if -nr in commandline
        if not no_results:
            file.close()
    # calculates all the error results for summary
    summary_info.set_all_error_totals('0A-CPR', '1A-AUX', product_to_check)
    if(verbosity >= 2):
        print("Writing Summary")

    # retrieves end time of program
    summary_info.end_time = time.strftime('%H:%M:%S')
    # reduces by half to counts errors for both 0A-CPR and 1A-AUX

    FMT = '%H:%M:%S'  # hour-minute-second format
    # end time - start time = execution time
    summary_info.execution_time = datetime.datetime.strptime(summary_info.end_time, FMT) - datetime.datetime.strptime(summary_info.start_time, FMT)
    # does not print if -ns in commandline
    if not no_summary:
        write_summary(product_to_check, summary_name, summary_info)
    if(verbosity >= 2):
        print("Finished!")
