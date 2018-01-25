import os
import re
import pandas as pd
from tqdm import tqdm


# for egn activity slice txt files
class Text_File_Processor:
    # Absolute
    # file_location = "D:/py/script/GGN NETWORK ANALYSIS_ver 1.0/data/row_data/"

    # Relative
    file_location = "data/row_data/"

    def __init__(self, file_name):
        def file_name_washer(f_name):
            filter_rules = [
                "Geopark_", "GEOPARK_", "geopark_"
                                        "Progress_", "PROGRESS_", "progress_"
                                                                  "Report", "REPORT", "report", "Reports", "REPORTS",
                "reports",
                ".txt"
            ]
            for rule in filter_rules:
                f_name = f_name.replace(rule, "")
            name = f_name.split("__")[0]
            time = f_name.split("__")[-1]
            return [name, time]

        clean_file_name = file_name_washer(file_name)

        self.file_name = clean_file_name[0]
        self.time = clean_file_name[1]
        self.file_address = self.file_location + file_name

        '''
        Things we will get
        '''
        self.contents = []  # a list contain each line
        self.df_titles = pd.DataFrame()  # a dataframe contains three part
        self.df = pd.DataFrame()  # a DF contains each activity in each line

    # txt file reader and remove the contacts which are irrelevant
    def open_file(self):
        # clean the line text
        # IMPORTANT
        def washer(line_text):
            # get ready
            line_text = line_text.strip()
            # replace
            filter_rules = [
                # clean form
                ("( ", "("), (" )", ")"),
                ("“ ", "“"), (" ”", "”"),
                ("' ", "'"),
                # remove
                ("'", "")
            ]
            # wash by rules
            for x, y in filter_rules:
                line_text = line_text.replace(x, y)
            # dry it
            while "  " in line_text:
                line_text = line_text.replace("  ", " ")

            return line_text.strip()

        # open files
        f = open(self.file_address, "r", encoding="utf16")

        contents = []
        for line in f:
            # read and clean line
            line = washer(line)
            # check is contact
            match = re.search(r'[\w\.-]+@[\w\.-]+', line)
            if match is not None:
                line = ""
            # no empty lines
            if line:
                contents.append(line)

        f.close()
        self.contents = contents
        return contents

# test

# test
    def organize_contents(self):
        def get_index(contents=None):
            if contents is None:
                # check which part is which part
                titles_GA = ('__BY_PARK',
                             'GEOPARK ACTIVITY', 'Geopark Activity',
                             'GEOPARK ACTIVITIES', 'Geopark Activities')
                titles_NT = ('__NETWORK', 'NETWORKING', 'Networking')
                titles_AP = ('__By_PARTNER',
                             'ACTIVITY BY PARTNER', 'Activity by Partner', 'Activity by partner',
                             'ACTIVITY BY PARTNERS', 'Activity by Partners', 'Activity by partners',
                             'ACTIVITIES BY PARTNERS', 'Activities by Partners', 'Activities by partners')
                titles = [titles_GA, titles_NT, titles_AP]
                output_index = []
                for p_in, line in enumerate(self.contents):

                    for title in titles:
                        if line.upper() in title:
                            output_index.append([p_in, title[0]])
                            # only can be find once
                            titles.remove(title)
                # create a dataframe for the list
                output_index = pd.DataFrame(output_index, columns=["LINES", "TYPE"]).sort_values(by=["LINES"])
                # get start : end index
                in_st_ls = output_index.LINES.tolist()
                in_en_ls = output_index.LINES.tolist()[1:]
                in_en_ls.append(None)
                # return the point
                output_index["LINES"] = list(zip(in_st_ls, in_en_ls))
            # ---  Nothing just a line --- #
            else:
                # is point line or not ???
                output_index = []
                for p_in, line in enumerate(contents):
                    #   small enough less than 4 :: 23.
                    logic_part = line.split(" ", maxsplit=1)[0]
                    logic_1 = len(logic_part) < 4
                    #   is number
                    logic_2 = logic_part[:-1].isdigit()
                    #   has .
                    logic_3 = "." in logic_part

                    logic = logic_1 and logic_2 and logic_3
                    if logic:
                        output_index.append(p_in)
                        # return [pi, pi2, pi3, pi4]

                # form slice
                in_st = output_index
                in_ed = output_index[1:]
                in_ed.append(None)
                output_index = list(zip(in_st, in_ed))

            return output_index

        def merge_line(title_index):
            # slice the contents
            contents = self.contents[title_index[0]: title_index[-1]]
            # go trough the index list of the contents
            output_merge = []
            point_index = get_index(contents)
            for in_st, in_ed in point_index:
                text = " ".join(contents[in_st:in_ed])
                text = text.split(". ", maxsplit=1)[-1]
                text = text.strip()

                output_merge.append(text)

            return output_merge

        df_titles = get_index()
        df_titles["CONTENTS"] = df_titles.LINES.apply(merge_line)
        # get title parts for output
        self.df_titles = df_titles
        # organize into dataframe
        df_act_ls = []
        for row_num in range(len(df_titles)):
            df_act = pd.DataFrame({
                "ACTIVITY": df_titles.CONTENTS[row_num].copy(),
            })
            df_act["TYPE"] = df_titles.TYPE[row_num]
            df_act_ls.append(df_act)
        df_act = pd.concat(df_act_ls)
        df_act["FILE_NAME"] = self.file_name
        df_act["TIME"] = self.time.replace("_", "/")
        # dataframe of thi file
        self.df = df_act

        return df_act

    def output_to_files(self, output_direction=None):
        if output_direction is None:
            output_direction = "data/mid_1_data/"
            direction_dict = {
                "__BY_PARK": "data/mid_1_data/GEOPARK_ACTIVITY/",
                "__NETWORK": "data/mid_1_data/NETWORKING/",
                "__By_PARTNER": "data/mid_1_data/ACTIVITY_BY_PARTNER/"
            }
        else:
            direction_dict = {
                "__BY_PARK": output_direction + "GEOPARK_ACTIVITY/",
                "__NETWORK": output_direction + "NETWORKING/",
                "__By_PARTNER": output_direction + "ACTIVITY_BY_PARTNER/"
            }

            for diction in list(direction_dict.values()):
                if not os.path.exists(diction):
                    os.makedirs(diction)

        # for output each files into divided things uncomment it if necessary
        '''
        for row_num in range(len(self.df_titles)):
            out_type = self.df_titles.TYPE[row_num]
            diction = direction_dict[out_type]
            file = self.file_name + self.time + out_type + ".txt"
            output_text = "\n".join(self.df_titles.CONTENTS[row_num])

            f = open(diction + file, "w", encoding="utf16")
            f.write(output_text)
            f.close()
        '''


        self.df.to_excel(output_direction + self.file_name + self.time + ".xlsx", index=False, encoding="utf16")
        pass



# build class for each file
print("--- Start Process Files ----")

f_ns = os.listdir("data/row_data/")
total_df_list = []
error_info = []
for f_n in tqdm(f_ns):

    try:
        file = Text_File_Processor(f_n)
        file.open_file()
        file.organize_contents()
        file.output_to_files()
        total_df_list.append(file.df)
    except:
        error_info.append(f_n)



f = open("data/mid_2_data/error_info.txt", "w")
f.write("These files may have problem:\n"+"\n".join(error_info))
f.close()
print("--- All Files Finished Writing Result File ---")
total_df = pd.concat(total_df_list)
total_df.to_excel("data/mid_2_data/progress_reports.xlsx", index=False)
print("--- Finished Output to mid_2_data ---")

