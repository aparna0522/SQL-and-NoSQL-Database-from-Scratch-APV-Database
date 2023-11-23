# https://www.npmjs.com/package/country-json
# https://github.com/samayo/country-json/tree/master/src

import json         # To deal with JSON NoSQL Data.
import os           # To deal with file creation and folder related operations.
import shutil       # To remove the temp created folders.
import warnings     # To remove any warnings.
warnings.filterwarnings('ignore')

# Load configuration parameters.
with open("config.json") as cfr:
    config_data = json.load(cfr)
chunk_size = config_data["chunk_size"]
hash_file_size = config_data["hash_file_size"]
temp_page_cnk_size = config_data["temp_page_cnk_size"]

################################################################################
# Function to open the json file.
def open_load_json(file_name):
    json_file = os.path.join(os.getcwd(), "{}.json".format(file_name))
    directory = os.path.join(os.getcwd(), "Data", (file_name.split("/")[-1].split("\\")[-1] + "_DB"))
    # Load the JSON file.
    if isinstance(json_file, str):
        with open(json_file, encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = json_file
    # Create the directory.
    if not os.path.exists(directory):
        os.makedirs(directory)
    return data, directory
################################################################################
# Function to create the Database directory and file structure.
def create_db_structure(file_name, chunk_size=chunk_size, hash_file_size=hash_file_size):
    data, directory = open_load_json(file_name)
    start = 0
    end = chunk_size
    total_itr = -(-len(data)//chunk_size)
    for i, itr in enumerate(range(total_itr)):
        chunk_list = []
        for entry in data[start:end]:
            chunk_list.append(entry)
        # Serializing json
        json_object = json.dumps(chunk_list, indent=4)
        # Writing to sample.json
        with open(os.path.join(directory, "Data_{}.json".format(i)), "w") as outfile:
            outfile.write(json_object)
        start = start + chunk_size
        end = end + chunk_size
    # Metadata check.
    check_metadata(directory, chunk_size)
    dynamic_hash_table(hash_file_size, directory)
    print("\nDatabase table structure successfully created for '{}.json'.\n".format(file_name.split("/")[-1].split("\\")[-1]))
################################################################################
# Function for Metadata file.
def check_metadata(directory, chunk_size):
    metadata_path = os.path.join(directory, "metadata.json")
    if not os.path.exists(metadata_path):
        with open(metadata_path, "w") as f:
            f.write("{}")
    mdata = {"available": [], "columns": []}
    col_names = []
    for data_file in os.listdir(directory):
        if (os.path.isfile(os.path.join(directory, data_file)) and (data_file.split(".")[0] != "metadata")):
            file_num = data_file.split(".")[0].split("_")[-1]
            with open(os.path.join(directory, data_file)) as d:
                data = json.load(d)
                if len(data) < chunk_size:
                    mdata["available"].append(file_num)
                    if len(data) != 0:
                        col_names = list(data[0].keys())
                        mdata["columns"] = col_names
    if len(mdata["available"]) == 0:
        mdata["available"].append(0)
    mdata["available"] = sorted(mdata["available"])
    with open(metadata_path, "w") as outfile:
        outfile.write(json.dumps(mdata, indent=4))
################################################################################
# Function to get column names.
def get_column_names(directory):
    metadata_path = os.path.join(directory, "metadata.json")
    with open(metadata_path, "r") as f:
        mdata = json.load(f)
    col_list = mdata["columns"]
    return col_list
################################################################################
# Function to hash the key and return index.
def hash_function(key, array_size):
    if isinstance(key, str):
        # A basic hash function using the sum of ASCII values of characters in the key.
        hash_value = sum(ord(char) for char in str(key))
    elif isinstance(key, int):
        hash_value = key
    else:
        hash_value = 0
    # Modulo operation to get an index within the array size.
    index = hash_value % array_size
    return index
################################################################################
# Function to address collision and put values into hash table.
def address_collision_and_hash(key, value, col_hash_path, col, hash_file_size, index, upd_index, capacity, hash_file_name="Hash_{}_{}.json"):
    while True:
        with open(os.path.join(col_hash_path, hash_file_name.format(col, get_hash_file_num(hash_file_size, index)))) as hfr1:
            hash_array = json.load(hfr1)
        if hash_array[index%hash_file_size][0] is not None: # Some value already exists, can be same key, can be different key.
            if hash_array[index%hash_file_size][0] == key:
                upd_index = index
            index = (index + 1) % capacity  # Addressing collision.
        else:
            break
    # Open the file where the current entry is to be put.
    with open(os.path.join(col_hash_path, hash_file_name.format(col, get_hash_file_num(hash_file_size, index)))) as hfr2:
        hash_array = json.load(hfr2)
    hash_array[index%hash_file_size] = [key, value, None]
    # Save that opened file.
    with open(os.path.join(col_hash_path, hash_file_name.format(col, get_hash_file_num(hash_file_size, index))), "w") as hfw1:
        hfw1.write(json.dumps(hash_array, indent=4))

    # Check if the original entry needs to be updated with linked index.
    if upd_index is not None:
        with open(os.path.join(col_hash_path, hash_file_name.format(col, get_hash_file_num(hash_file_size, upd_index)))) as hfr3:
            hash_array = json.load(hfr3)
        hash_array[upd_index%hash_file_size][2] = index
        with open(os.path.join(col_hash_path, hash_file_name.format(col, get_hash_file_num(hash_file_size, upd_index))), "w") as hfw2:
            hfw2.write(json.dumps(hash_array, indent=4))
################################################################################
# Function to perform re-hashing.
def rehash_all(key, value, old_capacity, col, hash_file_size, col_hash_path, hashmeta_path, hmdata, index, upd_index):
    new_free = old_capacity * 2
    new_capacity = old_capacity * 2

    new_hash_array = [[None, None, None] for i in range(hash_file_size)]
    existing_hash_file_nums = [i for i in range(0, get_hash_file_num(hash_file_size, old_capacity-1)+1)]
    new_temp_hash_file_nums = [i for i in range(0, len(existing_hash_file_nums)*2)]

    # Temp hash files creation.
    for num in new_temp_hash_file_nums:
        with open(os.path.join(col_hash_path, "Hash_{}_{}_temp.json".format(col, num)), "w") as hm:
            hm.write(json.dumps(new_hash_array, indent=4))

    # Re-hashing and transferring existing hash file to temp hash files.
    for num in existing_hash_file_nums:
        with open(os.path.join(col_hash_path, "Hash_{}_{}.json".format(col, num))) as ohf:
            old_hash_file = json.load(ohf)
        for kvl in old_hash_file:
            key = kvl[0]
            value = kvl[1]
            index = hash_function(key, new_capacity)
            upd_index = None
            hash_file_name = "Hash_{}_{}_temp.json"
            address_collision_and_hash(key, value, col_hash_path, col, hash_file_size, index, upd_index, new_capacity, hash_file_name)
            new_free = new_free - 1

    hmdata["total_free_indexes"] = new_free
    hmdata["total_current_capacity"] = new_capacity

    with open(hashmeta_path, "w") as hm:
        hm.write(json.dumps(hmdata, indent=4))

    # Delete old hash files.
    for num in existing_hash_file_nums:
        os.remove(os.path.join(col_hash_path, "Hash_{}_{}.json".format(col, num)))
    # Rename temp files to actual file names.
    for num in new_temp_hash_file_nums:
        os.rename(os.path.join(col_hash_path, "Hash_{}_{}_temp.json".format(col, num)), os.path.join(col_hash_path, "Hash_{}_{}.json".format(col, num)))

    return new_free, new_capacity
################################################################################
# Function to get hash file number.
def get_hash_file_num(hash_file_size, index):
    for i in range(0, 9):
        if hash_file_size*i <= index < hash_file_size*(i+1):
            return i
################################################################################
# Function to create dynamic hash table / file entries.
def dynamic_hash_table(hash_file_size, directory):
    col_list = get_column_names(directory)
    for col in col_list:
        col_hash_path = os.path.join(directory, "Hash_tables", col)
        hashmeta_path = os.path.join(col_hash_path, "hashmeta.json")
        hash_zero_path = os.path.join(col_hash_path, "Hash_{}_0.json".format(col))

        if not os.path.exists(col_hash_path):
            os.makedirs(col_hash_path)

        # Initiate the hash directory, hashmeta file, and hash array 0th file with default hash_file_size capacity.
        hmdata = {"total_free_indexes": hash_file_size, "total_current_capacity": hash_file_size}
        # Hash array structure: index0 = value, index1 = datafile number, index2 = next value occurance index.
        hash_array = [[None, None, None] for i in range(hash_file_size)]
        free = hmdata["total_free_indexes"]
        capacity = hmdata["total_current_capacity"]

        # Delete pre-existing hash-files.
        for hash_file in [hf for hf in os.listdir(col_hash_path) if os.path.isfile(os.path.join(col_hash_path, hf))]:
            os.remove(os.path.join(col_hash_path, hash_file))

        with open(hashmeta_path, "w") as hm:
            hm.write(json.dumps(hmdata, indent=4))

        with open(hash_zero_path, "w") as hf0:
            hf0.write(json.dumps(hash_array, indent=4))

        # Current-data Insert or Hash refresh.
        for data_file in [df for df in os.listdir(directory) if os.path.isfile(os.path.join(directory, df)) and (df.split(".")[0] != "metadata")]:
            with open(os.path.join(directory, data_file)) as d:
                data = json.load(d)
            #put the data into hash index.
            #data_file=Data_0.json
            #data=[{1},{2},{3},.....,{20}]
            #entry={"country":"Afganistan", "continent":"Asia", "id":1}
            #hash_array=[(None, None, None), (None, None, None), (None, None, None)]
            for entry in data:
                key = entry[col]    #Example: "Asia"
                value = data_file.split(".")[0].split("_")[-1]  #file_num: 0, 1, 2, ..., 12
                upd_index = None
                if free != 0:
                    # No need to re-hash, continue as it is.
                    index = hash_function(key, capacity)
                    address_collision_and_hash(key, value, col_hash_path, col, hash_file_size, index, upd_index, capacity)
                    free = free - 1
                    if free == 0:
                        free, capacity = rehash_all(key, value, capacity, col, hash_file_size, col_hash_path, hashmeta_path, hmdata, index, upd_index)
                else:
                    free, capacity = rehash_all(key, value, capacity, col, hash_file_size, col_hash_path, hashmeta_path, hmdata, index, upd_index)

        # Hashmeta update and write.
        hmdata["total_free_indexes"] = free
        hmdata["total_current_capacity"] = capacity
        with open(hashmeta_path, "w") as hm:
            hm.write(json.dumps(hmdata, indent=4))
################################################################################
# Function to create new DB / table.
def create_new_table_db(table_name, chunk_size=chunk_size):
    directory = os.path.join(os.getcwd(), "Data", (table_name + "_DB"))
    # Create the directory.
    if not os.path.exists(directory):
        os.makedirs(directory)
    check_metadata(directory, chunk_size)
    print("\nDatabase table structure '{0}_DB' successfully created.\n".format(table_name))
################################################################################
# Function to insert the values into DB.
def insert_into_db(table_name, values, chunk_size=chunk_size, hash_file_size=hash_file_size):
    directory = os.path.join(os.getcwd(), "Data", (table_name + "_DB"))
    metadata_path = os.path.join(directory, "metadata.json")
    with open(metadata_path, "r") as md:
        mdata = json.load(md)
    if len(mdata["available"]) == 0:
        insert_num = "0"
    else:
        insert_num = mdata["available"][0]

    insert_file_path = os.path.join(directory, "Data_{}.json".format(insert_num))
    if not os.path.exists(insert_file_path):
        fdata = []
    else:
        with open(insert_file_path, "r") as fd:
            fdata = json.load(fd)

    fdata.append(values)
    with open(insert_file_path, "w") as fd:
        fd.write(json.dumps(fdata, indent=4))
    check_metadata(directory, chunk_size)
    dynamic_hash_table(hash_file_size, directory)
    print("\nData successfully inserted in table '{}_DB'.\n".format(table_name))
################################################################################
# Function for hash search.
def hash_opt_search(directory, overall_data_file_nums, capacity, col_hash_path, search_col, search_val, hash_file_size, index):
    # overall_data_file_nums is an empty list when it gets inside this function.
    og_index = index
    while True:
        hfn = get_hash_file_num(hash_file_size, index)
        with open(os.path.join(col_hash_path, "Hash_{}_{}.json".format(search_col, hfn))) as hfr1:
            hash_array = json.load(hfr1)

        if hash_array[index%hash_file_size][0] != search_val:   # Match did not happen.
            index = (index + 1) % capacity
            if index == og_index:
                break

        else:   # Match located.
            while True:
                # Fetch the data_file number and store it in overall num list.
                data_file_num = hash_array[index%hash_file_size][1]
                if int(data_file_num) not in overall_data_file_nums:
                    overall_data_file_nums.append(int(data_file_num))
                # Check for next occurence.
                if hash_array[index%hash_file_size][2] is not None: # Next occurence exists.
                    index = hash_array[index%hash_file_size][2]
                    with open(os.path.join(col_hash_path, "Hash_{}_{}.json".format(search_col, get_hash_file_num(hash_file_size, index)))) as hfr2:
                        hash_array = json.load(hfr2)
                else: # Last occurance, exit now.
                    break

            overall_data_file_nums = sorted(overall_data_file_nums)
            return overall_data_file_nums
    return overall_data_file_nums
################################################################################
# Function to retrieve data from data file.
def retrieve_file_data(directory, data_file_num, search_col, search_val):
    with open(os.path.join(directory, "Data_{}.json".format(data_file_num))) as dfr1:
        file_data = json.load(dfr1)
    one_file_entries = []
    for entry in file_data:
        if entry[search_col] == search_val:
            one_file_entries.append(entry)
    return one_file_entries
################################################################################
# Function to make pages files for search results.
def pages_for_search(i_ea, ea, new_temp, del_temp, cnk_size=temp_page_cnk_size):
    #print(ea)
    temp_path = os.path.join(os.getcwd(), "temp")
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)
    tlist = sorted([int(dir.split("_")[-1]) for dir in os.listdir(temp_path) if os.path.isdir(os.path.join(temp_path, dir)) and dir.split("_")[0] == "temp"])

    if new_temp != 0:    # Create new temp.
        if len(tlist) == 0:
            folnum_create = 0
        else:
            folnum_create = tlist[-1] + 1
    else:   # Don't create new temp, continue in the current one every time this function is called.
        folnum_create = tlist[-1]
    tempi_path = os.path.join(temp_path, "temp_{}".format(folnum_create))
    if not os.path.exists(tempi_path):
        os.makedirs(tempi_path)

    # meta.txt file write.
    meta_list = []
    for i, col in enumerate(ea):
        dtv = "string"
        if type(ea[col]) == str:
            dtv = "string"
        elif type(ea[col]) == int:
            dtv = "integer"
        #meta_list.append("{0}{1}{2}{1}{3}\n".format(i, chr(170), col, dtv))
        meta_list.append(str(i) + chr(170) + str(col) + chr(170) + dtv + "\n")
    with open(os.path.join(tempi_path, "meta.txt"), "w") as mfw:
        mfw.writelines(meta_list)

    file_index = i_ea//cnk_size
    with open(os.path.join(tempi_path, "pages_{}.txt".format(file_index)), "a+") as pfile:
        pfile.write(chr(170).join(map(str, ea.values())) + "\n")
    return tempi_path
################################################################################
# Function to format search pages files into desirable syntax.
def include_brackets_to_pages(tempi_path):
    for page_file in [pf for pf in os.listdir(tempi_path) if os.path.isfile(os.path.join(tempi_path, pf)) and pf.split(".")[0] != "meta"]:
        with open(os.path.join(tempi_path, page_file), "r") as pfr:
            lines = pfr.readlines()
        for li, line in enumerate(lines):
            lines[li] = "{" + line.strip() + "}\n"
        with open(os.path.join(tempi_path, page_file), "w") as pfw:
           pfw.writelines(lines)
################################################################################
# Function to search in DB.
def search_in_db(table_name, search_col, operator, search_val, hash_file_size, where=0, del_temp="yes"):
    directory = os.path.join(os.getcwd(), "Data", (table_name + "_DB"))

    if where == 0:  # No Where condition, search whole table (default).
        new_temp = 1    # Create new temp.
        i_ea = 0
        for data_file_num in sorted([int(df.split(".")[0].split("_")[-1]) for df in os.listdir(directory) if os.path.isfile(os.path.join(directory, df)) and df.split(".")[0] != "metadata"]):
            with open(os.path.join(directory, "Data_{}.json".format(data_file_num))) as dfr:
                file_data = json.load(dfr)
            tempi_path = None
            if len(file_data) == 0:
                print("Empty set")
                return None, None
            for ea in file_data: 
                tempi_path = pages_for_search(i_ea, ea, new_temp, del_temp)
                new_temp = 0    # Disable new temp creation.
                i_ea = i_ea + 1
        include_brackets_to_pages(tempi_path)
        
        if del_temp == "yes":
            shutil.rmtree(tempi_path)
        return tempi_path, None

    col_hash_path = os.path.join(directory, "Hash_tables", search_col)
    hashmeta_path = os.path.join(col_hash_path, "hashmeta.json")

    with open(hashmeta_path) as hm:
        hmdata = json.load(hm)

    free = hmdata["total_free_indexes"]
    capacity = hmdata["total_current_capacity"]

    if operator.lower() == "in":
        master_data_file_nums_dict = {}
        new_temp = 1    # Create new temp.
        i_ea = 0
        for sv in search_val:   # search_val=["Asia", "Africa"]
            overall_data_file_nums = []
            master_data_file_nums_dict[sv] = []

            index = hash_function(sv, capacity)
            overall_data_file_nums = hash_opt_search(directory, overall_data_file_nums, capacity, col_hash_path, search_col, sv, hash_file_size, index)
            master_data_file_nums_dict[sv] = overall_data_file_nums
            if len(overall_data_file_nums) == 0:
                continue
            for data_file_num in overall_data_file_nums:
                one_file_entries = retrieve_file_data(directory, data_file_num, search_col, sv)
                for ea in one_file_entries:
                    tempi_path = pages_for_search(i_ea, ea, new_temp, del_temp)
                    new_temp = 0    # Disable new temp creation.
                    i_ea = i_ea + 1
        if len(overall_data_file_nums) == 0:
            if del_temp == "yes":
                shutil.rmtree(tempi_path)
            return None, master_data_file_nums_dict
        include_brackets_to_pages(tempi_path)
        if del_temp == "yes":
            shutil.rmtree(tempi_path)
        return tempi_path, master_data_file_nums_dict

    elif operator.lower() == "=":
        overall_data_file_nums = []
        index = hash_function(search_val, capacity)
        overall_data_file_nums = hash_opt_search(directory, overall_data_file_nums, capacity, col_hash_path, search_col, search_val, hash_file_size, index)
        if len(overall_data_file_nums) == 0:
            return None, overall_data_file_nums
        new_temp = 1    # Create new temp.
        i_ea = 0
        for data_file_num in overall_data_file_nums:
            one_file_entries = retrieve_file_data(directory, data_file_num, search_col, search_val)
            for ea in one_file_entries:
                tempi_path = pages_for_search(i_ea, ea, new_temp, del_temp)
                new_temp = 0    # Disable new temp creation.
                i_ea = i_ea + 1
        include_brackets_to_pages(tempi_path)
        if del_temp == "yes":
            shutil.rmtree(tempi_path)
        return tempi_path, overall_data_file_nums
    else:
        print("\nInvalid operator entered. Please enter operator in '=' or 'IN'.")
        quit()
        return None, None
################################################################################
# Function to update the DB.
def update_db_values(table_name, set_col, set_val, search_col, operator, search_val, hash_file_size=hash_file_size):
    tempi_path, data_file_nums = search_in_db(table_name, search_col, operator, search_val, hash_file_size, where=1)

    directory = os.path.join(os.getcwd(), "Data", (table_name + "_DB"))
    col_hash_path = os.path.join(directory, "Hash_tables", search_col)
    hashmeta_path = os.path.join(col_hash_path, "hashmeta.json")

    if isinstance(data_file_nums, dict):
        for sv in search_val:
            for file_num in data_file_nums[sv]:
                with open(os.path.join(directory, "Data_{}.json".format(file_num))) as dfr1:
                    file_data = json.load(dfr1)
                for ent in file_data:
                    if ent[search_col] == sv:
                        ent[set_col] = set_val
                with open(os.path.join(directory, "Data_{}.json".format(file_num)), "w") as dfw:
                    dfw.write(json.dumps(file_data, indent=4))

    elif isinstance(data_file_nums, list):
        for file_num in data_file_nums:
            with open(os.path.join(directory, "Data_{}.json".format(file_num))) as dfr1:
                file_data = json.load(dfr1)
            for ent in file_data:
                if ent[search_col] == search_val:
                    ent[set_col] = set_val
            with open(os.path.join(directory, "Data_{}.json".format(file_num)), "w") as dfw:
                dfw.write(json.dumps(file_data, indent=4))

    check_metadata(directory, chunk_size)
    dynamic_hash_table(hash_file_size, directory)
    print("\nData successfully updated in table '{}_DB'.\n".format(table_name))
################################################################################
# Function to delete from DB.
def delete_db_values(table_name, search_col, operator, search_val, hash_file_size=hash_file_size):
    tempi_path, data_file_nums = search_in_db(table_name, search_col, operator, search_val, hash_file_size, where=1)
    directory = os.path.join(os.getcwd(), "Data", (table_name + "_DB"))
    col_hash_path = os.path.join(directory, "Hash_tables", search_col)
    hashmeta_path = os.path.join(col_hash_path, "hashmeta.json")

    if isinstance(data_file_nums, dict):
        # Operator is 'IN'.
        for sv in search_val:
            for file_num in data_file_nums[sv]:
                with open(os.path.join(directory, "Data_{}.json".format(file_num))) as dfr1:
                    file_data = json.load(dfr1)
                file_data = [ent for ent in file_data if ent[search_col] != sv]
                with open(os.path.join(directory, "Data_{}.json".format(file_num)), "w") as dfw:
                    dfw.write(json.dumps(file_data, indent=4))
    elif isinstance(data_file_nums, list):
        # Operator is '='.
        for file_num in data_file_nums:
            with open(os.path.join(directory, "Data_{}.json".format(file_num))) as dfr1:
                file_data = json.load(dfr1)
            file_data = [ent for ent in file_data if ent[search_col] != search_val]
            with open(os.path.join(directory, "Data_{}.json".format(file_num)), "w") as dfw:
                dfw.write(json.dumps(file_data, indent=4))
    check_metadata(directory, chunk_size)
    dynamic_hash_table(hash_file_size, directory)
    print("\nData successfully deleted from table '{}_DB'.\n".format(table_name))
################################################################################
# Function to perform projection from DB.
def projection_from_db(table_name, projection_cols, search_col, operator, search_val, hash_file_size, where=0, del_temp="yes"):
    tempi_path, data_file_nums = search_in_db(table_name, search_col, operator, search_val, hash_file_size, where=where, del_temp="no")
    if tempi_path is None:
        #print("\nEmpty set.\n")
        return None
    if isinstance(projection_cols, str):    # ALL columns.
        with open(os.path.join(tempi_path, "meta.txt"), "r") as mfr:
             mlines = mfr.readlines()
        headers = [ml.split(chr(170))[1] for ml in mlines]

        for page_file in [pf for pf in os.listdir(tempi_path) if os.path.isfile(os.path.join(tempi_path, pf)) and pf.split(".")[0] != "meta"]:
            with open(os.path.join(tempi_path, page_file)) as pfr:
                pflines = pfr.readlines()          
            for line in pflines:
                ea = {}
                for i, h in enumerate(headers):
                    ea[h] = line[1:-2].split(chr(170))[i]
                #print(ea)
        if del_temp == "yes":
            shutil.rmtree(tempi_path)
            tempi_path = None

    elif isinstance(projection_cols, list): # List of column or columns.
        mtemp = []
        pindex = []
        with open(os.path.join(tempi_path, "meta.txt"), "r") as mfr:
             mlines = mfr.readlines()
        headers = [ml.split(chr(170))[1] for ml in mlines]
        for pc in projection_cols:
            for li, header in enumerate(headers):
                if pc == header:
                    mtemp.append(mlines[li])
                    pindex.append(li)
        with open(os.path.join(tempi_path, "meta.txt"), "w") as mfw:
           mfw.writelines(mtemp)

        for page_file in [pf for pf in os.listdir(tempi_path) if os.path.isfile(os.path.join(tempi_path, pf)) and pf.split(".")[0] != "meta"]:
            with open(os.path.join(tempi_path, page_file)) as pfr:
                pflines = pfr.readlines()          
            for line in pflines:
                ea = {}
                for i, h in enumerate(projection_cols):
                    ea[h] = [line[1:-2].split(chr(170))[j] for j in pindex][i]
                #print(ea)
        if del_temp == "yes":
            shutil.rmtree(tempi_path)
            tempi_path = None
    return tempi_path
################################################################################
# Function to perform order-by operation.
def order_by(table_name, projection_cols, search_col, operator, search_val, order_by_col, where=0, sort=1, desc=0, hash_file_size=hash_file_size, del_temp="yes", oprint=1):
    #os.system('PageManager.exe "order" "temp/temp_0" "country" "ASC" "20"')
    tempi_path = projection_from_db(table_name, projection_cols, search_col, operator, search_val, hash_file_size, where=where, del_temp="no")
    if tempi_path is None:
        print("\nEmpty set.\n")
        return None

    if sort == 1:   # Perform sorting.  
        tempi_folder = os.path.split(tempi_path)[-1]
        if desc == 0:   # Ascending sort (default).
            if os.name == "nt":
                os.system('PageManager.exe "order" "{}" "{}" "ASC" "{}"'.format("temp/"+tempi_folder, order_by_col, temp_page_cnk_size))
            else:
                os.system('./PageManager.o "order" "{}" "{}" "ASC" "{}"'.format("temp/"+tempi_folder, order_by_col, temp_page_cnk_size))
            #print("\n========== Ascending Order-by Result ==========\n")
        elif desc == 1: # Descending sort.
            if os.name == "nt":
                os.system('PageManager.exe "order" "{}" "{}" "DESC" "{}"'.format("temp/"+tempi_folder, order_by_col, temp_page_cnk_size))
            else:
                os.system('./PageManager.o "order" "{}" "{}" "DESC" "{}"'.format("temp/"+tempi_folder, order_by_col, temp_page_cnk_size))
            #print("\n========== Descending Order-by Result ==========\n")

    with open(os.path.join(tempi_path, "meta.txt"), "r") as mfr:
        mlines = mfr.readlines()
    headers = [ml.split(chr(170))[1] for ml in mlines]
    pindex = [int(ml.split(chr(170))[0]) for ml in mlines]

    if oprint == 1:
        print("")
        for page_file_num in sorted([int(pf.split(".")[0].split("_")[-1]) for pf in os.listdir(tempi_path) if os.path.isfile(os.path.join(tempi_path, pf)) and pf.split(".")[0] != "meta"]):
            with open(os.path.join(tempi_path, "pages_{}.txt".format(page_file_num))) as pfr:
                pflines = pfr.readlines()          
            for line in pflines:
                ea = {}
                for i, h in enumerate(headers):
                    #ea[h] = line[1:-2].split(chr(170))[i]
                    ea[h] = [line[1:-2].split(chr(170))[j] for j in pindex][i]
                print(ea)

    if del_temp == "yes":
        shutil.rmtree(tempi_path)
        tempi_path = None

    return tempi_path
################################################################################
# Function to join table_a result and table_b result.
def join_a_b(a_params, b_params, sort_after_join_col, sort_after_join=0, desc=0, hash_file_size=hash_file_size, del_temp="yes"):
    #os.system('PageManager.exe "join" "temp/temp_3" "temp/temp_4" "country" "=" "country" "temp/temp_2"')
    a_tempi_path = order_by(a_params["table_name"], a_params["projection_cols"], a_params["search_col"], a_params["operator"], a_params["search_val"], a_params["order_by_col"], where=a_params["where"], sort=a_params["sort"], desc=a_params["desc"], del_temp="no", oprint=0)
    b_tempi_path = order_by(b_params["table_name"], b_params["projection_cols"], b_params["search_col"], b_params["operator"], b_params["search_val"], b_params["order_by_col"], where=b_params["where"], sort=b_params["sort"], desc=b_params["desc"], del_temp="no", oprint=0)

    temp_path = os.path.split(b_tempi_path)[0]
    a_tempi_folder = os.path.split(a_tempi_path)[-1]
    b_tempi_folder = os.path.split(b_tempi_path)[-1]
    o_tempi_folder = "temp_" + str(int(b_tempi_folder.split("_")[-1]) + 1)

    a_tempi_path = os.path.join(temp_path, a_tempi_folder)
    b_tempi_path = os.path.join(temp_path, b_tempi_folder)
    o_tempi_path = os.path.join(temp_path, o_tempi_folder)

    if not os.path.exists(os.path.join(temp_path, o_tempi_folder)):
        os.makedirs(os.path.join(temp_path, o_tempi_folder))
    
    a_join_col = a_params["join_col"]
    b_join_col = b_params["join_col"]

    if os.name == "nt":
        os.system('PageManager.exe "join" "{}" "{}" "{}" "=" "{}" "{}" "{}"'.format("temp/"+a_tempi_folder, "temp/"+b_tempi_folder, a_join_col, b_join_col, "temp/"+o_tempi_folder, temp_page_cnk_size))
    else:
        os.system('./PageManager.o "join" "{}" "{}" "{}" "=" "{}" "{}" "{}"'.format("temp/"+a_tempi_folder, "temp/"+b_tempi_folder, a_join_col, b_join_col, "temp/"+o_tempi_folder, temp_page_cnk_size))


    # Removing previous temp folders.
    #shutil.rmtree(a_tempi_path)
    #shutil.rmtree(b_tempi_path)

    with open(os.path.join(o_tempi_path, "meta.txt"), "r") as mfr:
        mlines = mfr.readlines()
    headers = [ml.split(chr(170))[1] for ml in mlines]
    pindex = [int(ml.split(chr(170))[0]) for ml in mlines]

    if sort_after_join != 0:    # Sort the join output.
        if desc == 0:   # Ascending sort (default).
            if os.name == "nt":
                os.system('PageManager.exe "order" "{}" "{}" "ASC" "{}"'.format("temp/"+o_tempi_folder, sort_after_join_col, temp_page_cnk_size))
            else:
                os.system('./PageManager.o "order" "{}" "{}" "ASC" "{}"'.format("temp/"+o_tempi_folder, sort_after_join_col, temp_page_cnk_size))

            #print("\n========== Post-Join Ascending Order-by Result ==========\n")
        elif desc == 1: # Descending sort.
            if os.name == "nt":
                os.system('PageManager.exe "order" "{}" "{}" "DESC" "{}"'.format("temp/"+o_tempi_folder, sort_after_join_col, temp_page_cnk_size))
            else:
                os.system('./PageManager.o "order" "{}" "{}" "DESC" "{}"'.format("temp/"+o_tempi_folder, sort_after_join_col, temp_page_cnk_size))

            #print("\n========== Post-Join Descending Order-by Result ==========\n")
    else:
        #print("\n========== Join Result ==========\n")
        pass

    for page_file_num in sorted([int(pf.split(".")[0].split("_")[-1]) for pf in os.listdir(o_tempi_path) if os.path.isfile(os.path.join(o_tempi_path, pf)) and pf.split(".")[0] != "meta"]):
        with open(os.path.join(o_tempi_path, "pages_{}.txt".format(page_file_num))) as pfr:
            pflines = pfr.readlines()
        for line in pflines:
            ea = {}
            for i, h in enumerate(headers):
                ea[h] = [line[1:-2].split(chr(170))[j] for j in pindex][i]
            print(ea)
    
    if del_temp == "yes":
        shutil.rmtree(o_tempi_path)
        o_tempi_path = None
    print() 
################################################################################
# Function to group-by.
def group_by(table_name, projection_cols, search_col, operator, search_val, sort_after_group_col, grby_col, agg_present_list, where=0, sort_after_group=0, desc=0, hash_file_size=hash_file_size, del_temp="yes"):
    #os.system('PageManager.exe "grp_agg" "temp/temp_3" "continent" "country" "CNT" "20"')
    tempi_path = order_by(table_name, projection_cols, search_col, operator, search_val, sort_after_group_col, where=where, sort=0, desc=desc, del_temp="no", oprint=0)
    tempi_folder = os.path.split(tempi_path)[-1]
    if os.name == "nt":
        os.system('PageManager.exe "order" "{}" "{}" "ASC" "{}"'.format("temp/"+tempi_folder, grby_col, temp_page_cnk_size))
        os.system('PageManager.exe "grp_agg" "{}" "{}" "{}" "{}"'.format("temp/"+tempi_folder, grby_col, '" "'.join(agg_present_list), temp_page_cnk_size))
    else:
        os.system('./PageManager.o "order" "{}" "{}" "ASC" "{}"'.format("temp/"+tempi_folder, grby_col, temp_page_cnk_size))
        os.system('./PageManager.o "grp_agg" "{}" "{}" "{}" "{}"'.format("temp/"+tempi_folder, grby_col, '" "'.join(agg_present_list), temp_page_cnk_size))


    with open(os.path.join(tempi_path, "meta.txt"), "r") as mfr:
        mlines = mfr.readlines()
    headers = [ml.split(chr(170))[1] for ml in mlines]
    pindex = [int(ml.split(chr(170))[0]) for ml in mlines]

    if sort_after_group == 1:
        tempi_folder = os.path.split(tempi_path)[-1]
        if desc == 0:   # Ascending sort (default).
            if os.name == "nt":
                os.system('PageManager.exe "order" "{}" "{}" "ASC" "{}"'.format("temp/"+tempi_folder, sort_after_group_col, temp_page_cnk_size))
            else:
                os.system('./PageManager.o "order" "{}" "{}" "ASC" "{}"'.format("temp/"+tempi_folder, sort_after_group_col, temp_page_cnk_size))

            #print("\n========== Post-Group Ascending Order-by Result ==========\n")
        elif desc == 1: # Descending sort.
            if os.name == "nt":
                os.system('PageManager.exe "order" "{}" "{}" "DESC" "{}"'.format("temp/"+tempi_folder, sort_after_group_col, temp_page_cnk_size))
            else:
                os.system('./PageManager.o "order" "{}" "{}" "DESC" "{}"'.format("temp/"+tempi_folder, sort_after_group_col, temp_page_cnk_size))

            #print("\n========== Post-Group Descending Order-by Result ==========\n")
    else:
        #print("\n========== Group-by Result ==========\n")
        pass

    for page_file_num in sorted([int(pf.split(".")[0].split("_")[-1]) for pf in os.listdir(tempi_path) if os.path.isfile(os.path.join(tempi_path, pf)) and pf.split(".")[0] != "meta"]):
        with open(os.path.join(tempi_path, "pages_{}.txt".format(page_file_num))) as pfr:
            pflines = pfr.readlines()
        for line in pflines:
            ea = {}
            for i, h in enumerate(headers):
                ea[h] = [line[1:-2].split(chr(170))[j] for j in pindex][i]
            print(ea)

    if del_temp == "yes":
        shutil.rmtree(tempi_path)
        tempi_path = None
################################################################################

# To run the NoSQL Database independently without the CLI, uncomment the following lines and run 
# python3 nosql_src.py (Ensure that your directory contains config.json before executing)

# Create DB structure.
# file_name = "country-by-continent"
# create_db_structure(file_name)

# Create NEW Table / DB.
# table_name = "students"
#create_new_table_db(table_name, chunk_size)

# Insert values.
# table_name = "country-by-continent"
# values = {"country": "Verdansk", "continent": "Asia", "id": 245}
# insert_into_db(table_name, values, chunk_size, hash_file_size)

# Search algorithm.
# table_name = "country-by-abbreviation"
# where = 1
# search_col = "country"
# operator = "in"
# search_val = ["India", "China"]
# del_temp = "no"
# tempi_path, data_file_nums = search_in_db(table_name, search_col, operator, search_val, hash_file_size, where=where, del_temp=del_temp)

# Update value.
# table_name = "students"
# set_col = "name"
# set_val = "Teresa"
# search_col = "name"
# operator = "="
# search_val = "Pbtribk"
# update_db_values(table_name, set_col, set_val, search_col, operator, search_val, hash_file_size)

# Delete value.
# table_name = "students"
# search_col = "sid"
# operator = "="
# search_val = 3
# delete_db_values(table_name, search_col, operator, search_val, hash_file_size)

# Projection operation.
# table_name = "students"
# projection_cols = ["id", "name"] # Or a list of columns meant to be projected.
# where = 0
# search_col = "continent"
# operator = "in"
# search_val = ["Asia", "Africa"]
# del_temp = "no"
# tempi_path = projection_from_db(table_name, projection_cols, search_col, operator, search_val, hash_file_size, where=where, del_temp=del_temp)

# Ordering operation.
# table_name = "students"
# projection_cols = "all" # Or a list of columns meant to be projected.
# where = 0
# search_col = "continent"
# operator = "in"
# search_val = ["Asia", "Africa"]
# sort = 1
# order_by_col = "name"
# desc = 0
# del_temp = "no"
# tempi_path = order_by(table_name, projection_cols, search_col, operator, search_val, order_by_col, where=where, sort=sort, desc=desc, hash_file_size=hash_file_size, del_temp=del_temp)

# Join operation.
# a_params = {
#             "table_name": "students",
#             "projection_cols": ["id", "name"],
#             "where": 0,
#             "search_col": "currency_code",
#             "operator": "IN",
#             "search_val": ["USD", "EUR"],
#             "sort": 0,
#             "order_by_col": "country",
#             "desc": 0,
#             "join_col": "name"}
# b_params = {
#             "table_name": "students",
#             "projection_cols": ["name", "address"],
#             "where": 0,
#             "search_col": "currency_name",
#             "operator": "=",
#             "search_val": "US Dollar",
#             "sort": 0,
#             "order_by_col": "country",
#             "desc": 0,
#             "join_col": "name"}

# del_temp = "no"
# sort_after_join = 0
# sort_after_join_col = "s.currency_code"
# desc = 0
# join_a_b(a_params, b_params, sort_after_join_col, sort_after_join=sort_after_join, desc=desc, del_temp=del_temp)

# Group & Aggregate operation.
# table_name = "students"
# projection_cols = ["address", "name"] # Or a list of columns meant to be projected.
# where = 0
# search_col = "continent"
# operator = "in"
# search_val = ["Asia", "Africa"]
# sort_after_group = 0
# sort_after_group_col = "continent"
# desc = 1

# grby_col = "address"
# agg_col = "name"
# agg_opp = "CNT"
# agg_present_list = ["name", "CNT"]

# del_temp = "no"
# group_by(table_name, projection_cols, search_col, operator, search_val, sort_after_group_col, grby_col, agg_present_list, where=where, sort_after_group=sort_after_group, desc=desc, hash_file_size=hash_file_size, del_temp=del_temp)
# os.system('PageManager.exe "grp_agg" "<temp folder path, expected items sorted by the group by column>" "<group by attribute>" "<aggregate attribute name>" "<aggregate operation>" "<page_chunk_size>"')
