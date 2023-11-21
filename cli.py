print()

import time
import re
import os
import json
import nosql_src as nosql # Import your functions from the other Python file

header = """
-------------------------------- Welcome to --------------------------------------
                            
    ___    ____ _    __            ____        __        __                       
   /   |  / __ \\ |  / /           / __ \\____ _/ /_____ _/ /_  ____ _________      
  / /| | / /_/ / | / /  ______   / / / / __ `/ __/ __ `/ __ \\/ __ `/ ___/ _ \\     
 / ___ |/ ____/| |/ /  /_____/  / /_/ / /_/ / /_/ /_/ / /_/ / /_/ (__  )  __/     
/_/  |_/_/     |___/           /_____/\\__,_/\\__/\\__,_/_.___/\\__,_/____/\\___/  

                                                                    version 1.0.0


----------------------------------------------------------------------------------                                                                                                                                                                                                                                                                                      
"""                                                                                                                                         
print(header)

nosql_workspace = os.getcwd()
sql_workspace = os.path.join(os.getcwd(), "sql_workspace")

sql_actions = {
    "load": ["load data in <table_name> with <path> generate PrimaryKeyValues provided headers", 
             "\t- <table_name> is the table name you want to load data in", 
             "\t- <path> is the absolute path to the csv file you wish to load",
             "\t- 'generate PrimaryKeyValues' add this if you wish to append row number at start of each entry",
             "\t- 'provided headers' enter this if the .csv file contains headers as the first row"],
    "list": ["list tables / list table <table_name>",
             "\t- list tables: list all tables present in the database",
             "\t- list table <table_name>: will list all columns of the table, and give thier types and if it is primary key"
            ],
    "define": ["define table <table_name> with [[<column_name>, <column_type>, <primary_key>], ...]",
             "\t- <table_name> is the table name you want create",
             "\t- <column_name> is the name of a column in table",
             "\t- <column_type> is the type of a entries the column will have (integer, string or float)",
             "\t- <primary_key> mention 'PrimaryKey' if <column_name> is a primary key",
             "\t- repeat [<column_name>, <column_type>, <primary_key>] to define more tables"
             ],
    "fill": ["fill table <table_name> with values [<val_1>, <val_2>, ...]",
             "\t- <table_name> is the table name you want create",
             "\t- <val_x> are the values to be inserted in table, if string wrap value with ''\n\t these values are needed to be in same order as defined while creating table"
            ],
    "edit": ["edit table <table_name> with values <set_column_name> = <set_attribute_value>, ... provided <search_col> <operator> <search_val>",
             "\t- <table_name> is the table name you want edit the entries of",
             "\t- <set_column_name> column name of which the entries you want edit",
             "\t- <set_column_value> value of the given <set column name>",
             "\t- <search_col> edit the entries in table if some conditions are true for this column",
             "\t- <operator> condition on the search_col",
             "\t- <search_value> value the search column needs to compare with\n\n\t you can update many columns at once using ',' sepearator.\n\t provided section is optional"
            ],
    "remove": ["remove from table <table_name> provided <search_col> <operator> <search_val>",
             "\t- <table_name> is the table name you want remove the entries of",
             "\t- <search_col> remove the entries in table if some conditions are true for this column",
             "\t- <operator> condition on the search_col",
             "\t- <search_value> value the search column needs to compare with\n\n\t provided section is optional"
            ],
    "find": ["find all from <table_name> provided <search_col> <operator> <search_val> sorting <sort_col> <sort_order>",
             "find [<projected_col>, <projected_col>, ...] from <table_name> provided <search_col> <operator> <search_val> sorting <sort_col> <sort_order>",
             "find [<aggregation_type>(<col_name>), ...] from <table_name> provided <search_col> <operator> <search_val> sorting <sort_col> <sort_order> cluster on <cluster_col>",
             "\t- <table_name> is the table you wish to query the entries from",
             "\t- <search_col> query the entries in table if some conditions are true for this column",
             "\t- <operator> condition on the search_col",
             "\t- <search_value> value the search column needs to compare with",
             "\t- <sort_col> column you wish to perform sorting on",
             "\t- <sort_order> order of the sort, ASC/DESC",
             "\t- <projected_col> columns you want to project in final query output",
             "\t- <aggregation_type> can be CNT, SUM, AVG, MIN, or MAX",
             "\t- <cluster_col> column name you want to cluster your results with\n\t provided, sorting, cluster on clauses are optional"
            ],
    "merge" : ["merge (<find_clause>) as <identifier> with (<find_clause>) as <identifier> on <identifier>.<col_name> <operator> <identifier>.<col_name> sorting <sort_col> <sort_order>",
               "\t- <find_clause> see find for detailed explaination\n\t find [<projected_col>, <projected_col>, ...] from <table_name> provided <search_col> <operator> <search_val> sorting <sort_col> <sort_order>",
               "\t- <identifier> name give to output returned from <find_clause>",
               "\t- <identifier>.<col_name> column name from <identifier> table you wish to perform merge on"
               "\t- <sort_col> column you wish to perform final sorting on",
               "\t- <sort_order> order of the sort, ASC/DESC\n\t final sorting is optional",
               ]
}

no_sql_actions = {
    "load": ["load data <json_file>",
             "\t- <json_file> absolute path to json file, will generate table with file name and load all data"],
    "list": ["list tables",
             "\t- will list all tables present in the database"],
    "define": ["define table <table_name>",
             "\t- creates a table with the name <table_name> with empty entries"],
    "fill": ["fill table <table_name> with values {<dictionary>}",
             "\t- <table_name> is the table you want to insert the entry in",
             "\t- <dictionary> is the value in json format (kay, value) to be inserted in the table"
             ],
    "edit": ["edit table <table_name> with values <set_key> = <set_val> provided <serach_key> = <search_val>",
             "\t- <table_name> is the table you want to update the entries of",
             "\t- <set_key> name of the key in table you want to update",
             "\t- <set_value> value of the key after edit operation is done",
             "\t- <serach_key> edit the entries in table if some conditions are true for this key",
             "\t- <search_val> value the search key needs to be equal with"
             ],
    "remove": ["remove from table <table_name> provided <serach_key> = <search_val>",
             "\t- <table_name> is the table you want to remove the entries from",
             "\t- <serach_key> remove the entries in table if some conditions are true for this key",
             "\t- <search_val> value the search key needs to be equal with"
             ],
    "find": ["find all from <table_name> provided <serach_key> <operator> <search_val> sorting <sorting_key> <sorting_order>",
             "find [projected_keys] from <table_name> provided <serach_key> <operator> <search_val> sorting <sorting_key> <sorting_order>"
             "find [<aggregation_type>(key)] from <table_name> provided <serach_key> <operator> <search_val> sorting <sorting_key> <sorting_order> cluster on <cluster_key>"
             "\t- <table_name> is the table you wish to query the entries from",
             "\t- <search_key> query the entries in table if some conditions are true for this key",
             "\t- <operator> condition on the search_key, can be '=' or 'in'",
             "\t- <search_val> value the search column needs to compare with, if 'in' <seach_val> is provided in []",
             "\t- <sort_key> key you wish to perform sorting on",
             "\t- <sort_order> order of the sort, ASC/DESC",
             "\t- <projected_key> keys you want to project in final query output",
             "\t- <aggregation_type> can be CNT, SUM, AVG, MIN, or MAX",
             "\t- <cluster_key> key name you want to cluster your results with\n\t provided, sorting, cluster on clauses are optional"
             ],
    "merge": ["merge (<find_clause>) with (<find_clause>) on <keyA> = <keyB> sorting <sorting_key> <sorting_order>",
              "\t- <find_clause> see find for detailed explaination\n\t find [projected_keys] from <table_name> provided <serach_key> <operator> <search_val> sorting <sorting_key> <sorting_order>",
              "\t- <keyA> = <keyB> key A from first table and key B from second table you wish to perform merge on",
              "\t- <sorting_key> key you wish to finally perform sorting on",
              "\t- <sorting_order> order of the sort, ASC/DESC\n\t final sorting is optional",
             ],
}

def main():   
    while True:
        using_db = input("Enter the Database type you wish to work on ('SQL' or 'NoSQL'):")
        if using_db.lower() == "sql":
            intro = "APV-SQL>"
            os.chdir(sql_workspace)
            break
        elif using_db.lower() == "nosql":
            intro = "APV-NoSQL>"
            os.chdir(nosql_workspace)
            break
        elif using_db.lower() == "exit":
            print("\nExiting the APV-Database CLI. Goodbye! 👋 \n\n")
            time.sleep(1)
            quit(exit)
        else:
            print("\nInvalid input. Please enter either 'SQL' or 'NoSQL'")
            continue

    while True:
        user_input = ""
        line = ""
        prints = intro
        while True:
            line = input("{}".format(prints)).strip()
            if(line.endswith(';')):
                user_input += " " + line[:-1]
                break
            else:
                user_input += " " + line
            
            if(len(line) > 0):
                prints = ''.ljust(len(intro)-2, ' ') + "-> "
            else:
                continue

        user_input = user_input.strip()

        if user_input.lower() == "exit":
            print("\nExiting the APV-Database CLI. Goodbye! 👋 \n\n")
            time.sleep(1)
            break

        elif user_input.lower() == "chdb":
            main()
            break
            
        elif user_input.lower() == "":
            continue

        if intro == "APV-NoSQL>":
            nosql_process_user_command(user_input)
        elif intro == "APV-SQL>":
            sql_process_user_command(user_input)

def try_int_conversion(cval):
    try:
        cval = int(cval)
    except:
        pass
    return cval

def join_params_retrieval(table_statement):
    # table_statement: find ["star", "galaxy"] from solar_system provided planets in [9, 12] sorting planets asc
    parts = table_statement.split()
    args = parts[1:]

    table_name = args[args.index("from") + 1]    
    if(not os.path.exists(os.path.join(os.getcwd(), 'Data')) or not os.path.exists(os.path.join(os.getcwd(), 'Data', table_name))):
        print("Table with name '{}' does not exist".format(table_name))
        return None, False
    if args[0] == "all":
        projection_cols = "all"
    else:
        projection_cols = json.loads(re.findall(r'\[.*?\]', table_statement)[0])
    where = 0
    search_col = None
    operator = None
    search_val = None
    if "provided" in args:
        where = 1
        search_col = args[args.index("provided") + 1]
        operator = args[args.index("provided") + 2]
        if operator == "=":
            search_val = args[args.index("provided") + 3]
            search_val = try_int_conversion(search_val)
        elif operator.lower() == "in":
            search_val = json.loads(re.findall(r'\[.*?\]', table_statement)[-1])
            search_val = [try_int_conversion(i) for i in search_val]
        else:
            print('operator in provided clause is invalid. Valid operators are "=" or "in"')
            return None, False
    sort = 0
    order_by_col = None
    desc = 0
    if "sorting" in args:
        sort = 1
        order_by_col = args[args.index("sorting") + 1]
        if args[-1] == "desc":
            desc = 1
    params_d ={
            "table_name": table_name,
            "projection_cols": projection_cols,
            "where": where,
            "search_col": search_col,
            "operator": operator,
            "search_val": search_val,
            "sort": sort,
            "order_by_col": order_by_col,
            "desc": desc}
    return params_d, True

def sql_merge_query_retriever(f_statement):
    # f_statement = find [] from <tableName> //provided <where_col> <where_op> <where_val> //sorting <sort_col> ASC/DESC //cluster on <group_col> 

    # Split the user command into parts
    parts = f_statement.split()

    # Extract command and arguments
    action = parts[0].lower()
    args = parts[1:]

    table_name = args[args.index("from") + 1]
    if(not os.path.exists(os.path.join(os.getcwd(), "databases")) or not os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
        print("Table with name '{}' does not exist".format(table_name))
        return None, False
            
    grby_col = ""
    if "cluster" not in args:
        if args[0] == "all":
            projection_cols = "all"
            col_num = 0
        else:
            projection_cols = json.loads(re.findall(r'\[.*?\]', f_statement)[0])
            agg_os_entry = '" "'.join(projection_cols)
            col_num = len(projection_cols)       
    elif "cluster" in args:
        projection_list = json.loads(re.findall(r'\[.*?\]', f_statement)[0])
        grby_col = args[args.index("cluster") + 2]
        agg_list = projection_list
        projection_cols = [grby_col]
        agg_present_list = []
        for i, af in enumerate(agg_list):
            agg_col = re.findall(r'\(.*?\)', af)[0][1:-1]
            agg_opp = af[:3]
            agg_present_list.append(agg_col)
            agg_present_list.append(agg_opp)
            projection_cols.append(agg_col)
        for i in range(1, len(agg_present_list), 2):
            if agg_present_list[i] not in ["SUM", "CNT", "AVG", "MAX", "MIN"]:
                print('Invalid aggregation operation, supported aggregation operations are: "SUM", "CNT", "AVG", "MAX", "MIN"')
                return None, False
        agg_os_entry = '" "'.join(agg_present_list)
        col_num = len(agg_present_list)
    search_col = ""
    operator = ""
    search_val = ""
    if "provided" in args:
        search_col = args[args.index("provided") + 1]
        operator = args[args.index("provided") + 2]
        if operator not in ["=","<",">","<=",">="]:
            print("Invalid operators in provided clause for the table {}".format(table_name))
            return None, False
        search_val = args[args.index("provided") + 3]
        if search_val.startswith('"'):
            whole_val = ""
            for i in range(args.index("provided")+3, len(args)):
                if args[i].endswith("\""):
                    whole_val += " " + args[i]
                    break
                whole_val += " " + args[i]
            search_val = whole_val.strip()
        if search_val[0] in ['"', "'"] and search_val[-1] in ['"', "'"]:
            search_val = search_val[1:-1] 
    sort_col = ""
    sort_type = ""
    if "sorting" in args:
        sort_col = args[args.index("sorting") + 1]
        if args[args.index("sorting") + 2] == "ASC":
            sort_type = "ASC"
        elif args[args.index("sorting") + 2] == "DESC":
            sort_type = "DESC"
    if col_num == 0:
        f_format_entry = '"{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, col_num, search_col, operator, search_val, sort_col, sort_type, grby_col)
    else:
        f_format_entry = '"{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, col_num, agg_os_entry, search_col, operator, search_val, sort_col, sort_type, grby_col)
    return f_format_entry, True

def print_table(headers, data):
    max_col_width = [
        len(max(item, key=len))
        for item in [list(group) for group in zip(headers, *data)]
    ]
    separator = "+".join("".ljust(width+2, '-') for width in max_col_width)
    
    print("+" + separator + "+")
    print(f"|{headers[0].ljust(max_col_width[0]+2, ' ')}|{headers[1].ljust(max_col_width[1]+2, ' ')}|{headers[2].ljust(max_col_width[2]+2, ' ')}|")
    print("+" + separator + "+")

    for column_name, column_type, pk in data:
        print(f"|{column_name.ljust(max_col_width[0]+2)}|{column_type.ljust(max_col_width[1]+2)}|{pk.ljust(max_col_width[2]+2)}|")
    print("+" + separator + "+")

def sql_process_user_command(command):
    # Split the user command into parts
    parts = command.split()

    # Extract command and arguments
    action = parts[0].lower()
    args = parts[1:]

    if action == "load":
        # Example: load data in <table_Name> with <path> generate PrimaryKey provided headers
        if( len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            table_name = args[2]
            if(not os.path.exists(os.path.join(os.getcwd(), 'databases')) or not os.path.exists(os.path.join(os.getcwd(), 'databases', table_name))):
                print("Table with name '{}' does not exist".format(table_name))
                return
            file_name = args[args.index("with") + 1]
            if file_name[0] in ['"', "'"] and file_name[-1] in ['"', "'"]:
                file_name = file_name[1:-1]
            if "generate" in args:
                pk_flag = ""
            else:
                pk_flag = "true"
            if "provided" in args:
                hd_flag = "true"
            else:
                hd_flag = ""
            if os.name == "nt":
                os.system('RelationalDBAPI.exe "load_data" "{}" "{}" "{}" "{}"'.format(table_name, file_name, pk_flag, hd_flag))
            else:
                os.system('./RelationalDB.o "load_data" "{}" "{}" "{}" "{}"'.format(table_name, file_name, pk_flag, hd_flag))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "--help":
        commands = "\n\n- ".join(vals[0] for vals in sql_actions.values())
        print("\n- "+commands+"\n")
    
    elif action == "list":
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            #example: list tables
            if args[0] == "tables":
                if(os.path.exists(os.path.join(os.getcwd(), "databases"))):
                    tables = os.listdir(os.path.join(os.getcwd(), "databases"))
                    tables_str = '\t'.join(tables)
                    print(tables_str)
                else:
                    print()
            elif args[0] == "table":
                table_name = args[1]
                if(not os.path.exists(os.path.join(os.getcwd(), "databases")) or not os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
                    print("Table with name '{}' does not exist".format(table_name))
                    return
                file_path = os.path.join(os.getcwd(), 'databases', table_name, 'meta.txt')
                with open(file_path, 'r', encoding='unicode_escape') as file:
                    file_content = file.read()
                if file_content == '':
                    print("Table with name '{}' does not exist".format(table_name))

                lines = file_content.split('\n')
                data = [line.split(chr(170)) for line in lines if chr(170) in line]
                pks = lines[-1].split()[1].split(",")
                data = [item for item in data if item]
                for i, ele in enumerate(data):
                    if ele[0] in pks:
                        ele.append("True")
                    else:
                        ele.append("False")

                print_table(["column names", "column type", "primary key"], data)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "define":
        # Example: define table <tableName> with [["planet_sr_no", "integer", "PrimaryKey"], ["moons", "integer"], ["suns", "integer"], ["layers", "integer"]]
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            table_name = args[1]
            if(os.path.exists(os.path.join(os.getcwd(), "databases")) and os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
                    print("Table with name '{}' already exists".format(table_name))
                    return
            atr_list = json.loads(command.split("with")[-1].strip())
            atr_present_list = []
            if(len(atr_list) == 0):
                print("Please enter atleast one column to create '{}' table".format(table_name))
                return
            for atr in atr_list:
                if len(atr) == 3 and atr[-1].lower() == "primarykey":
                    for ea in atr[:-1]:
                        atr_present_list.append(ea)
                    atr_present_list.append("PK")
                    if(atr[1] not in ["string", "integer", "float"]):
                        print("Invalid column type for the '{}' column".format(atr[0]))
                        return
                elif len(atr) == 2:
                    for ea in atr:
                        atr_present_list.append(ea)
                    atr_present_list.append("")
                    if(atr[1] not in ["string", "integer", "float"]):
                        print("Invalid column type for the '{}' column".format(atr[0]))
                        return
                else:
                    print("Columns are in invalid format, expected either [<col_name>, <col_type>, 'primaryKey'] or [<col_name>, <col_type>]")
                    return
            attributes = '" "'.join(atr_present_list)
            if os.name == "nt":
                os.system('RelationalDBAPI.exe "define_table" "{}" "{}"'.format(table_name, attributes))
            else:
                os.system('./RelationalDB.o "define_table" "{}" "{}"'.format(table_name, attributes))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print("'{}' table created successfully".format(table_name))
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
            
    elif action == "fill":
        # Example: fill table <table_name> with values [1, 'earth', '24hrs', '365days', 1]
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            table_name = args[1]
            if(not os.path.exists(os.path.join(os.getcwd(), "databases")) or not os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
                    print("Table with name '{}' does not exist".format(table_name))
                    return
            values = json.loads(command.split("values")[-1].strip())
            attributes = '" "'.join(map(str, values))
            if os.name == "nt":
                os.system('RelationalDBAPI.exe "fill_table" "{}" "{}"'.format(table_name, attributes))
            else:
                os.system('./RelationalDB.o "fill_table" "{}" "{}"'.format(table_name, attributes))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print("Filled {} with the entry successfully".format(table_name))
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "edit":
        # Example: edit table <table_name> with values <set_column_name>=<set_attribute_value>, <set_column_name>=<set_attribute_value>, ... provided <search_col> <operator> <search_value>
        # Example: edit table <table_Name> with values name = 'mars', duration = '48hrs', moon = 0 provided name = 'earth'
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            table_name = args[1]
            if(not os.path.exists(os.path.join(os.getcwd(), "databases")) or not os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
                    print("Table with name '{}' does not exist".format(table_name))
                    return
            if "provided" in command:
                si = command.find("values")
                ei = command.find("provided")
                upd_string = command[si+len("values") : ei]
                #upd_string =  " name = 'mars', duration = '48hrs', moon = 0"
                upd_raw_list = [i.split("=") for i in upd_string.split(",")]
                #upd_raw_list = [[' name ', " 'mars'"], [' duration ', " '48hrs'"], [' moon ', ' 0 ']]
                upd_present_list = []
                for i in upd_raw_list:
                    for ele in i:
                        if ele.strip()[0] in ['"', "'"] and ele.strip()[-1] in ['"', "'"]:
                            upd_present_list.append(ele.strip()[1:-1])
                        else:
                            upd_present_list.append(ele.strip())
                #upd_present_list = ['name', 'mars', 'duration', '48hrs', 'moon', '0']
                search_col = args[args.index("provided") + 1]
                operator = args[args.index("provided") + 2]
                if operator not in ["=","<",">","<=",">="]:
                    print("Invalid operators in provided clause for the table {}".format(table_name))
                    return
                search_val = args[args.index("provided") + 3]
                if search_val.startswith('"'):
                    whole_val = ""
                    for i in range(args.index("provided")+3, len(args)):
                        if args[i].endswith("\""):
                            whole_val += " " + args[i]
                            break
                        whole_val += " " + args[i]
                    search_val = whole_val.strip()
                if search_val[0] in ['"', "'"] and search_val[-1] in ['"', "'"]:
                    search_val = search_val[1:-1]
            else:
                si = command.find("values")
                upd_string = command[si+len("values") :]
                #upd_string =  " name = 'mars', duration = '48hrs', moon = 0"
                upd_raw_list = [i.split("=") for i in upd_string.split(",")]
                #upd_raw_list = [[' name ', " 'mars'"], [' duration ', " '48hrs'"], [' moon ', ' 0 ']]
                upd_present_list = []
                for i in upd_raw_list:
                    for ele in i:
                        if ele.strip()[0] in ['"', "'"] and ele.strip()[-1] in ['"', "'"]:
                            upd_present_list.append(ele.strip()[1:-1])
                        else:
                            upd_present_list.append(ele.strip())
                #upd_present_list = ['name', 'mars', 'duration', '48hrs', 'moon', '0']
                search_col = ""
                operator = ""
                search_val = ""
            attributes = '" "'.join(map(str, upd_present_list))
            if os.name == "nt":
                os.system('RelationalDBAPI.exe "edit_table" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, len(upd_raw_list), attributes, search_col, operator, search_val))
            else:
                os.system('./RelationalDB.o "edit_table" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, len(upd_raw_list), attributes, search_col, operator, search_val))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "remove":
        # Example: remove from table <table_name> provided <search_col> <operator> <search_value>
        # Example: remove from table <table_Name> provided name >= 'earth'
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("table") + 1]
            if(not os.path.exists(os.path.join(os.getcwd(), "databases")) or not os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
                    print("Table with name '{}' does not exist".format(table_name))
                    return
            search_col = ""
            operator = ""
            search_val = ""
            if "provided" in args:
                search_col = args[args.index("provided") + 1]
                operator = args[args.index("provided") + 2]
                if operator not in ["=","<",">","<=",">="]:
                    print("Invalid operators in provided clause for the table {}".format(table_name))
                    return
                search_val = args[args.index("provided") + 3]
                if search_val.startswith('"'):
                    whole_val = ""
                    for i in range(args.index("provided")+3, len(args)):
                        if args[i].endswith("\""):
                            whole_val += " " + args[i]
                            break
                        whole_val += " " + args[i]
                    search_val = whole_val.strip()
                if search_val[0] in ['"', "'"] and search_val[-1] in ['"', "'"]:
                    search_val = search_val[1:-1]
            if os.name == "nt":
                os.system('RelationalDBAPI.exe "remove_element" "{}" "{}" "{}" "{}"'.format(table_name, search_col, operator, search_val))
            else:
                os.system('./RelationalDB.o "remove_element" "{}" "{}" "{}" "{}"'.format(table_name, search_col, operator, search_val))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "find":
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("from") + 1]
            if(not os.path.exists(os.path.join(os.getcwd(), "databases")) or not os.path.exists(os.path.join(os.getcwd(), "databases", table_name))):
                    print("Table with name '{}' does not exist".format(table_name))
                    return
            grby_col = ""
            if "cluster" not in args:
                if args[0] == "all":
                    projection_cols = "all"
                    col_num = 0
                else:
                    projection_cols = json.loads(re.findall(r'\[.*?\]', command)[0])
                    agg_os_entry = '" "'.join(projection_cols)
                    col_num = len(projection_cols)       
            elif "cluster" in args:
                projection_list = json.loads(re.findall(r'\[.*?\]', command)[0])
                grby_col = args[args.index("cluster") + 2]
                agg_list = projection_list
                projection_cols = [grby_col]
                agg_present_list = []
                for i, af in enumerate(agg_list):
                    agg_col = re.findall(r'\(.*?\)', af)[0][1:-1]
                    agg_opp = af[:3]
                    agg_present_list.append(agg_col)
                    agg_present_list.append(agg_opp)
                    projection_cols.append(agg_col)
                for i in range(1, len(agg_present_list), 2):
                    if agg_present_list[i] not in ["SUM", "CNT", "AVG", "MAX", "MIN"]:
                        print('Invalid aggregation operation, supported aggregation operations are: "SUM", "CNT", "AVG", "MAX", "MIN"')
                        return
                agg_os_entry = '" "'.join(agg_present_list)
                col_num = len(agg_present_list)
            search_col = ""
            operator = ""
            search_val = ""
            if "provided" in args:
                search_col = args[args.index("provided") + 1]
                operator = args[args.index("provided") + 2]
                if operator not in ["=","<",">","<=",">="]:
                    print("Invalid operators in provided clause for the table {}".format(table_name))
                    return
                search_val = args[args.index("provided") + 3]
                if search_val.startswith('"'):
                    whole_val = ""

                for i in range(args.index("provided")+3, len(args)):
                    if args[i].endswith("\""):
                        whole_val += " " + args[i]
                        break
                    whole_val += " " + args[i]
                search_val = whole_val.strip()

                if search_val[0] in ['"', "'"] and search_val[-1] in ['"', "'"]:
                    search_val = search_val[1:-1] 
            sort_col = ""
            sort_type = ""
            if "sorting" in args:
                sort_col = args[args.index("sorting") + 1]
                args[args.index("sorting") + 2] = args[args.index("sorting") + 2].upper()
                if args[args.index("sorting") + 2] == "ASC":
                    sort_type = "ASC"
                elif args[args.index("sorting") + 2] == "DESC":
                    sort_type = "DESC"
                else: 
                    print("Invalid sorting order provided. Sorted the output in ascending order.")
                    sort_type = "ASC"

            if col_num == 0:
                if os.name == "nt":
                    os.system('RelationalDBAPI.exe "find_element" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, col_num, search_col, operator, search_val, sort_col, sort_type, grby_col))
                else:
                    os.system('./RelationalDB.o "find_element" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, col_num, search_col, operator, search_val, sort_col, sort_type, grby_col))
            else:
                if os.name == "nt":
                    os.system('RelationalDBAPI.exe "find_element" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, col_num, agg_os_entry, search_col, operator, search_val, sort_col, sort_type, grby_col))
                else:
                    os.system('./RelationalDB.o "find_element" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'.format(table_name, col_num, agg_os_entry, search_col, operator, search_val, sort_col, sort_type, grby_col))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "merge":
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        # Example: 
        try:
            start_time = time.time()
            a_statement = re.findall(r'\(.*?\)', command)[0][1:-1]
            b_statement = re.findall(r'\(.*?\)', command)[1][1:-1]
            a_format_entry, res_a = sql_merge_query_retriever(a_statement)
            b_format_entry, res_b = sql_merge_query_retriever(b_statement)
            if not res_a or not res_b:
                return
            a_as = command.split("as")[1].split()[0].strip()
            b_as = command.split("as")[2].split()[0].strip()
            col_x = command.split("on")[-1].split()[0]
            on_op = command.split("on")[-1].split()[1]
            if on_op not in ["=","<",">","<=",">="]:
                print("Invalid on operators in the on clause")
                return
            col_y = command.split("on")[-1].split()[2]
            if col_x.split(".")[0] == a_as:
                column_first = col_x.split(".")[1]
                column_second = col_y.split(".")[1]
            elif col_x.split(".")[0] == b_as:
                column_first = col_y.split(".")[1]
                column_second = col_x.split(".")[1]
            
            final_sortCol = ""
            final_sort_order = ""
            if args[-3] == "sorting":
                final_sortCol = args[-2]
                final_sort_order = args[-1]
            if os.name == "nt":
                os.system('RelationalDBAPI.exe "merge_tables" {} "{}" {} "{}" "{}" "{}" "{}" "{}" "{}"'.format(a_format_entry, a_as, b_format_entry, b_as, column_first, on_op, column_second, final_sortCol, final_sort_order))
            else:
                os.system('./RelationalDB.o "merge_tables" {} "{}" {} "{}" "{}" "{}" "{}" "{}" "{}"'.format(a_format_entry, a_as, b_format_entry, b_as, column_first, on_op, column_second, final_sortCol, final_sort_order))
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    else:
        print("Invalid command. See '--help' for proper usage.")

def nosql_process_user_command(command):
    # Split the user command into parts
    parts = command.split()

    # Extract command and arguments
    action = parts[0].lower()
    args = parts[1:]

    # Call the appropriate function based on the command
    if action == "load":
        # Example: load data country-by-continent.json
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[1].split(".")[0]
            nosql.create_db_structure(table_name)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "--help":
        commands = "\n\n- ".join(vals[0] for vals in no_sql_actions.values())
        print("\n\n- " + commands + "\n")

    elif action == "list":
        #example: list tables
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            if args[0] == "tables":
                if(not os.path.exists(os.path.join(os.getcwd(), 'Data'))):
                    print()
                    end_time = time.time()
                    delta_time_seconds = end_time - start_time
                    print(f"[{delta_time_seconds:.3f} seconds]")
                    return
                tables = os.listdir(os.path.join(os.getcwd(), "Data"))
                tables = [t[0:-3] for t in tables]
                tables_str = '\t'.join(tables)
                print(tables_str)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "define":
        # Example: define table solar_system
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[1].split(".")[0]
            if(os.path.exists(os.path.join(os.getcwd(), 'Data')) and os.path.exists(os.path.join(os.getcwd(), 'Data', table_name))):
                print("Table with name '{}' already exists".format(table_name))
                return
            nosql.create_new_table_db(table_name)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        
    elif action == "fill":
        # Example: fill table solar_system with values {"star": "sun", "galaxy": "milky way", "planets": 9}
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("table") + 1]
            if(not os.path.exists(os.path.join(os.getcwd(), 'Data')) or not os.path.exists(os.path.join(os.getcwd(), 'Data', table_name+"_DB"))):
                print("Table with name '{}' does not exist".format(table_name))
                return
            values = json.loads(re.findall(r'\{.*?\}', command)[0])
            nosql.insert_into_db(table_name, values)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "edit":
        # Example: edit table solar_system with values planets = 8 provided star = sun
        # Example: edit table solar_system with values planets = 12 provided star in ["name", "sun"]
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("table") + 1]
            if(not os.path.exists(os.path.join(os.getcwd(), 'Data')) or not os.path.exists(os.path.join(os.getcwd(), 'Data', table_name+"_DB"))):
                print("Table with name '{}' does not exist".format(table_name))
                return
            set_col = args[args.index("values") + 1]
            set_val = args[args.index("values") + 3]
            set_val = try_int_conversion(set_val)
            search_col = args[args.index("provided") + 1]
            operator = args[args.index("provided") + 2]
            if operator == "=":
                search_val = args[args.index("provided") + 3]
                search_val = try_int_conversion(search_val)
            elif operator.lower() == "in":
                search_val = json.loads(re.findall(r'\[.*?\]', command)[0])
                search_val = [try_int_conversion(i) for i in search_val]   
            else:
                print('operator in provided clause is invalid. Valid operators are "=" or "in"')
                return
            
            nosql.update_db_values(table_name, set_col, set_val, search_col, operator, search_val)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "remove":
        # Example: remove from table solar_system provided star = name
        # Example: remove from table solar_system provided planets in [9, 12]
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("table") + 1]
            if(not os.path.exists(os.path.join(os.getcwd(), 'Data')) or not os.path.exists(os.path.join(os.getcwd(), 'Data', table_name+"_DB"))):
                print("Table with name '{}' does not exist".format(table_name))
                return
            search_col = args[args.index("provided") + 1]
            operator = args[args.index("provided") + 2]
            if operator == "=":
                search_val = args[args.index("provided") + 3]
                search_val = try_int_conversion(search_val)
            elif operator.lower() == "in":
                search_val = json.loads(re.findall(r'\[.*?\]', command)[0])
                search_val = [try_int_conversion(i) for i in search_val]
            else:
                print('operator in provided clause is invalid. Valid operators are "=" or "in"')
                return
            print(table_name, search_col, operator, search_val)
            nosql.delete_db_values(table_name, search_col, operator, search_val)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        
    elif action == "find" and "cluster" not in args:
        # Example: find all from solar_system
        # Example: find all from solar_system provided star = sun
        # Example: find ["star", "galaxy"] from solar_system provided planets = 9
        # Example: find all from solar_system provided planets in [9, 12]
        # Example: find ["star", "galaxy"] from solar_system provided planets in [9, 12] sorting planets asc
        # Example: find all from solar_system provided planets in [9, 12] sorting planets desc
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("from") + 1]      
            if(not os.path.exists(os.path.join(os.getcwd(), 'Data')) or not os.path.exists(os.path.join(os.getcwd(), 'Data', table_name+"_DB"))):
                print("Table with name '{}' does not exist".format(table_name))
                return 
            if args[0] == "all":
                projection_cols = "all"
            else:
                projection_cols = json.loads(re.findall(r'\[.*?\]', command)[0])
            where = 0
            search_col = None
            operator = None
            search_val = None
            if "provided" in args:
                where = 1
                search_col = args[args.index("provided") + 1]
                operator = args[args.index("provided") + 2]
                if operator == "=":
                    search_val = args[args.index("provided") + 3]
                    search_val = try_int_conversion(search_val)
                elif operator.lower() == "in":
                    search_val = json.loads(re.findall(r'\[.*?\]', command)[-1])
                    search_val = [try_int_conversion(i) for i in search_val]
                else:
                    print('operator in provided clause is invalid. Valid operators are "=" or "in"')
                    return
            sort = 0
            order_by_col = None
            desc = 0
            if "sorting" in args:
                sort = 1
                order_by_col = args[args.index("sorting") + 1]
                args[-1] = args[-1].lower()
                if args[-1] == "desc":
                    desc = 1
            nosql.order_by(table_name, projection_cols, search_col, operator, search_val, order_by_col, where=where, sort=sort, desc=desc)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "find" and "cluster" in args:    # Group-by.
        # Example: find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting continent
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            table_name = args[args.index("from") + 1]
            if(not os.path.exists(os.path.join(os.getcwd(), 'Data')) or not os.path.exists(os.path.join(os.getcwd(), 'Data', table_name+"_DB"))):
                print("Table with name '{}' does not exist".format(table_name))
                return
            projection_list = json.loads(re.findall(r'\[.*?\]', command)[0])
            grby_col = args[args.index("cluster") + 2]
            agg_list = projection_list
            projection_cols = [grby_col]
            agg_present_list = []
            for i, af in enumerate(agg_list):
                agg_col = re.findall(r'\(.*?\)', af)[0][1:-1]
                agg_opp = af[:3]
                agg_present_list.append(agg_col)
                agg_present_list.append(agg_opp)
                projection_cols.append(agg_col)
            where = 0
            search_col = None
            operator = None
            search_val = None
            if "provided" in args:
                where = 1
                search_col = args[args.index("provided") + 1]
                operator = args[args.index("provided") + 2]
                if operator == "=":
                    search_val = args[args.index("provided") + 3]
                    search_val = try_int_conversion(search_val)
                elif operator.lower() == "in":
                    search_val = json.loads(re.findall(r'\[.*?\]', command)[-1])
                    search_val = [try_int_conversion(i) for i in search_val]
                else:
                    print('operator in provided clause is invalid. Valid operators are "=" or "in"')
                    return
            sort_after_group = 0
            sort_after_group_col = None
            desc = 0
            if "sorting" in args:
                sort_after_group = 1
                sort_after_group_col = args[args.index("sorting") + 1]
                args[-1] = args[-1].lower()
                if args[-1] == "desc":
                    desc = 1
            nosql.group_by(table_name, projection_cols, search_col, operator, search_val, sort_after_group_col, grby_col, agg_present_list, where=where, sort_after_group=sort_after_group, desc=desc)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return

    elif action == "merge":
        # Example: merge (find all from country-by-currency-code) with (find all from country-by-currency-name) on country = country
        # Example: merge (find ["country", "currency_code"] from country-by-currency-code) with (find ["country", "currency_name"] from country-by-currency-name) on country = country sorting s.currency_code asc
        if(len(args) > 0 and args[0] == "--help"):
            commands_help = "\n".join(vals for vals in no_sql_actions[action])
            print(commands_help + "\n")
            return
        try:
            start_time = time.time()
            a_statement = re.findall(r'\(.*?\)', command)[0][1:-1]
            b_statement = re.findall(r'\(.*?\)', command)[1][1:-1]
            a_params, res_a = join_params_retrieval(a_statement)
            b_params, res_b = join_params_retrieval(b_statement)
            if not res_a or not res_b:
                return
            a_join_col = args[args.index("on") + 1]
            b_join_col = args[args.index("on") + 3]
            a_params["join_col"] = a_join_col
            b_params["join_col"] = b_join_col
            sort_after_join = 0
            sort_after_join_col = None
            desc = 0
            if args[-3] == "sorting":
                sort_after_join = 1
                sort_after_join_col = args[-2]
                if args[-1] == "desc":
                    desc = 1
            nosql.join_a_b(a_params, b_params, sort_after_join_col, sort_after_join=sort_after_join, desc=desc)
            end_time = time.time()
            delta_time_seconds = end_time - start_time
            print(f"[{delta_time_seconds:.3f} seconds]")
        except:
            print("Invalid usage of this query, the proper usage is -")
            commands_help = "\n".join(vals for vals in sql_actions[action])
            print(commands_help+"\n")
            return
        
    else:
        print("Invalid command. See '--help' for proper usage.")

if __name__ == "__main__":
    main()